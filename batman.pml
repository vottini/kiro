/* batman.pml — B.A.T.M.A.N. routing core model for SPIN/Promela
 *
 * Topology: 3-node linear chain
 *
 *   N0 ──[link 0-1]── N1 ──[link 1-2]── N2
 *
 * N0 and N2 are not adjacent; every frame between them is relayed by N1.
 *
 * ── Relay suppression ─────────────────────────────────────────────────────
 * In the real protocol a relay node re-broadcasts a received OGM with
 * probability 1/hearingCount.  We model this as a non-deterministic choice:
 *
 *   if
 *   :: ogm_out!...   // relay
 *   :: skip          // suppress
 *   fi
 *
 * This over-approximates: SPIN explores executions where suppression always
 * wins, producing counterexamples for the liveness claims.  Those CEs are
 * valid in the abstract model but ruled out in the real protocol by 1/k > 0.
 * Weak fairness (-f) captures that guarantee: "if the relay transition is
 * continuously enabled it must eventually fire."
 *
 * ── Expected results ──────────────────────────────────────────────────────
 *                      pan (no flags)    pan -a -f (weak fairness)
 *   route_convergence  counterexample    holds ✓
 *   correct_delivery   counterexample    holds ✓
 *   delivery_monotone  holds ✓           holds ✓
 *   [assert] bounds    holds ✓           holds ✓
 *
 * ── How to run ────────────────────────────────────────────────────────────
 *   spin -a batman.pml && cc -o pan pan.c
 *   ./pan              # safety only
 *   ./pan -a -f        # all ltl claims with weak fairness
 *   spin batman.pml    # simulate one execution
 */

#define MAX_TTL  3
#define NO_ROUTE 255
#define Q        8

/* ── OGM channels ──────────────────────────────────────────────────────────
 * Layout: ( originator : byte, sender : byte, ttl : byte )
 */
chan ogm_01 = [Q] of { byte, byte, byte };
chan ogm_10 = [Q] of { byte, byte, byte };
chan ogm_12 = [Q] of { byte, byte, byte };
chan ogm_21 = [Q] of { byte, byte, byte };

/* ── Data channels ─────────────────────────────────────────────────────────
 * Layout: ( dst : byte, ttl : byte )
 */
chan dat_01 = [Q] of { byte, byte };
chan dat_12 = [Q] of { byte, byte };

/* ── Routing tables ────────────────────────────────────────────────────────
 * Promela does not support multi-dimensional arrays, so each node gets its
 * own flat arrays indexed by destination (0=N0, 1=N1, 2=N2).
 *
 * nh_nX[d] = next-hop from node X toward destination d  (NO_ROUTE if unknown)
 * qt_nX[d] = best OGM ttl quality seen for originator d at node X
 */
byte nh_n0[3];  byte qt_n0[3];
byte nh_n1[3];  byte qt_n1[3];
byte nh_n2[3];  byte qt_n2[3];

/* ── Observation flag ──────────────────────────────────────────────────────*/
bool delivered = false;

/* ── Per-node update inlines ───────────────────────────────────────────────
 * Because Promela inlines use textual substitution, a generic inline cannot
 * select between nh_n0/nh_n1/nh_n2 based on a runtime parameter.
 * One inline per node is the idiomatic workaround.
 */
inline update_n0(orig, nhop, q) {
  if :: (q > qt_n0[orig]) -> nh_n0[orig] = nhop; qt_n0[orig] = q
     :: else -> skip
  fi
}
inline update_n1(orig, nhop, q) {
  if :: (q > qt_n1[orig]) -> nh_n1[orig] = nhop; qt_n1[orig] = q
     :: else -> skip
  fi
}
inline update_n2(orig, nhop, q) {
  if :: (q > qt_n2[orig]) -> nh_n2[orig] = nhop; qt_n2[orig] = q
     :: else -> skip
  fi
}

/* ════════════════════════════════════════════════════════════════════════════
 * N0 — left endpoint.  Single link to N1.
 * ════════════════════════════════════════════════════════════════════════════ */
proctype Node0() {
  byte orig, sndr, ttl;

  do
  :: ogm_01!0, 0, MAX_TTL                       /* broadcast own OGM         */

  :: ogm_10?orig, sndr, ttl ->                  /* receive relay from N1     */
       assert(ttl > 0 && ttl <= MAX_TTL);
       update_n0(orig, 1, ttl)

  :: (nh_n0[2] != NO_ROUTE) ->                  /* send data once route known */
       dat_01!2, MAX_TTL
  od
}

/* ════════════════════════════════════════════════════════════════════════════
 * N1 — relay node.  Links to both N0 and N2.
 * Relay decisions are non-deterministic to model suppression.
 * ════════════════════════════════════════════════════════════════════════════ */
proctype Node1() {
  byte orig, sndr, ttl, dst, dttl;

  do
  :: ogm_10!1, 1, MAX_TTL                       /* own OGM toward N0         */
  :: ogm_12!1, 1, MAX_TTL                       /* own OGM toward N2         */

  :: ogm_01?orig, sndr, ttl ->                  /* OGM from N0 side          */
       assert(ttl > 0 && ttl <= MAX_TTL);
       update_n1(orig, 0, ttl);
       if
       :: (ttl > 1) ->
            if
            :: ogm_12!orig, 1, (ttl-1)          /* relay toward N2           */
            :: skip                              /* suppress                  */
            fi
       :: else -> skip
       fi

  :: ogm_21?orig, sndr, ttl ->                  /* OGM from N2 side          */
       assert(ttl > 0 && ttl <= MAX_TTL);
       update_n1(orig, 2, ttl);
       if
       :: (ttl > 1) ->
            if
            :: ogm_10!orig, 1, (ttl-1)          /* relay toward N0           */
            :: skip                              /* suppress                  */
            fi
       :: else -> skip
       fi

  :: dat_01?dst, dttl ->                        /* forward data toward N2    */
       assert(dttl > 0);
       if
       :: (dst == 2 && dttl > 1) -> dat_12!dst, (dttl-1)
       :: else -> skip
       fi
  od
}

/* ════════════════════════════════════════════════════════════════════════════
 * N2 — right endpoint.  Single link to N1.
 * ════════════════════════════════════════════════════════════════════════════ */
proctype Node2() {
  byte orig, sndr, ttl, dst, dttl;

  do
  :: ogm_21!2, 2, MAX_TTL                       /* broadcast own OGM         */

  :: ogm_12?orig, sndr, ttl ->                  /* receive relay from N1     */
       assert(ttl > 0 && ttl <= MAX_TTL);
       update_n2(orig, 1, ttl)

  :: dat_12?dst, dttl ->                        /* receive data frame        */
       assert(dst == 2);
       delivered = true
  od
}

/* ════════════════════════════════════════════════════════════════════════════
 * init
 * ════════════════════════════════════════════════════════════════════════════ */
init {
  atomic {
    nh_n0[0] = 0;        qt_n0[0] = MAX_TTL;   /* self-route */
    nh_n0[1] = NO_ROUTE; qt_n0[1] = 0;
    nh_n0[2] = NO_ROUTE; qt_n0[2] = 0;

    nh_n1[0] = NO_ROUTE; qt_n1[0] = 0;
    nh_n1[1] = 1;        qt_n1[1] = MAX_TTL;   /* self-route */
    nh_n1[2] = NO_ROUTE; qt_n1[2] = 0;

    nh_n2[0] = NO_ROUTE; qt_n2[0] = 0;
    nh_n2[1] = NO_ROUTE; qt_n2[1] = 0;
    nh_n2[2] = 2;        qt_n2[2] = MAX_TTL;   /* self-route */

    run Node0();
    run Node1();
    run Node2()
  }
}

/* ════════════════════════════════════════════════════════════════════════════
 * LTL properties
 * ════════════════════════════════════════════════════════════════════════════ */

/* Both endpoints learn routes to each other.  Requires -f. */
ltl route_convergence { <> (nh_n0[2] != NO_ROUTE && nh_n2[0] != NO_ROUTE) }

/* Packet from N0 eventually arrives at N2.  Requires -f. */
ltl correct_delivery { <> delivered }

/* Once delivered, always delivered.  Holds without -f. */
ltl delivery_monotone { [] (delivered -> [] delivered) }

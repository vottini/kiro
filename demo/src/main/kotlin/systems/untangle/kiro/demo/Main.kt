package systems.untangle.kiro.demo

import kotlinx.coroutines.*
import systems.untangle.kiro.GroupId
import systems.untangle.kiro.KiroRouter
import java.io.BufferedReader
import java.io.InputStreamReader
import java.nio.file.Path
import kotlin.system.exitProcess
import kotlin.time.Duration.Companion.seconds

/**
 * Usage:
 *   java -jar kiro.jar <nodeId> <medium1> [<medium2> ...]
 *
 * Each medium path is a directory that acts as a shared broadcast channel.
 * Any node that lists the same directory can hear every other node on it.
 * Nodes are discovered automatically — no neighbor list required.
 *
 * Example — three nodes sharing two overlapping media:
 *
 *   java -jar kiro.jar 1 /tmp/net-a
 *   java -jar kiro.jar 2 /tmp/net-a /tmp/net-b
 *   java -jar kiro.jar 3 /tmp/net-b
 *
 * Here 1 and 2 share net-a, 2 and 3 share net-b.
 * Node 1 can reach node 3 through node 2 after routing converges.
 */
fun main(args: Array<String>) {
    if (args.size < 2) {
        System.err.println("usage: kiro.jar <nodeId> <medium> [<medium>...]")
        return
    }

    val selfId  = args[0].toUShort()
    val mediums = args.drop(1).map { Path.of(it) }

    val links = mediums.map { mediumPath ->
        PipeLink(
            id          = mediumPath.fileName.toString(),
            mediumDir   = mediumPath,
            selfId      = selfId,
            ogmInterval = 2.seconds,
        ).also { it.initialize() }
    }

    val router = KiroRouter(selfId = selfId, links = links)

    println("=== kiro node $selfId ===")
    mediums.forEach { println("  medium: $it") }
    println("type 'help' for commands\n")

    runBlocking {
        links.forEach { it.startReading(this) }
        router.start(this)

        launch {
            router.incomingData.collect { (srcId, payload) ->
                println("[unicast from $srcId] ${payload.decodeToString()}")
            }
        }

        launch {
            router.incomingMulticast.collect { msg ->
                println("[mcast from ${msg.srcId} group ${msg.groupId.id}] ${msg.payload.decodeToString()}")
            }
        }

        val stdin = BufferedReader(InputStreamReader(System.`in`))
        while (isActive) {
            val line = withContext(Dispatchers.IO) { stdin.readLine() } ?: break
            try {
                handleCommand(line.trim(), router, links)
            } catch (e: Throwable) {
                System.err.println("ERROR in handleCommand: ${e::class.qualifiedName}: ${e.message}")
                e.printStackTrace(System.err)
            }
        }

        exitProcess(0)
    }
}

private suspend fun handleCommand(
    line: String,
    router: KiroRouter,
    links: List<PipeLink>
) {
    if (line.isBlank()) return
    val parts = line.split(" ", limit = 3)
    when (parts[0].lowercase()) {

        "send" -> {
            if (parts.size < 3) { println("usage: send <dstId> <message>"); return }
            router.send(parts[1].toUShort(), parts[2].encodeToByteArray())
        }

        "mcast" -> {
            if (parts.size < 3) { println("usage: mcast <groupId> <message>"); return }
            router.sendMulticast(GroupId(parts[1].toUInt()), parts[2].encodeToByteArray())
        }

        "join" -> {
            if (parts.size < 2) { println("usage: join <groupId> [<rootId>]"); return }
            val gid   = GroupId(parts[1].toUInt())
            val roots = if (parts.size >= 3) listOf(parts[2].toUShort()) else listOf(router.selfId)
            router.joinGroup(gid, roots)
            if (router.selfId in roots) println("joined group ${gid.id} as root")
            else println("joined group ${gid.id} as member (root=${roots.first()})")
        }

        "leave" -> {
            if (parts.size < 2) { println("usage: leave <groupId>"); return }
            val gid = GroupId(parts[1].toUInt())
            if (router.leaveGroup(gid)) println("left group ${gid.id}")
            else println("not a member of group ${gid.id}")
        }

        "routes" -> {
            val routes = router.routes()
            if (routes.isEmpty()) println("(no routes yet)")
            else routes.entries
                .sortedBy { it.key.toInt() }
                .forEach { (dst, e) ->
                    println("  dst=%-5s nextHop=%-5s medium=%-12s ttl=%d"
                        .format(dst, e.nextHop, e.link.id, e.bestTtl.toInt()))
                }
        }

        "offline" -> {
            links.forEach { it.online = false }
            println("offline — all media suppressed")
        }

        "online" -> {
            links.forEach { it.online = true }
            println("online")
        }

        "help" -> println(HELP)

        "quit", "exit" -> exitProcess(0)

        else -> println("unknown command '${parts[0]}' — type 'help'")
    }
    return
}

private val HELP = """
commands:
  send  <dstId> <message>      unicast message to node
  mcast <groupId> <message>    multicast to group
  join  <groupId> [<rootId>]   join group (omit rootId to join as root)
  leave <groupId>              leave group
  routes                       show routing table
  offline                      simulate going offline (all media)
  online                       come back online
  quit / exit                  exit
""".trimIndent()

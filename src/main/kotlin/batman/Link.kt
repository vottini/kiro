package batman

import kotlinx.coroutines.flow.Flow
import kotlin.time.Duration

typealias NodeId = UShort

interface Link {
    val id: String
    val ogmInterval: Duration

    suspend fun broadcast(frame: ByteArray)
    val frames: Flow<ByteArray>
}

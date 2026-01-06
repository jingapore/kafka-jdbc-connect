package jdbcConnector.utils

import java.nio.file.Files
import java.nio.file.Path

object PathUtils {
    fun getProjectRoot(): Path {
        var current = Path.of(System.getProperty("user.dir"))
        while (current != null) {
            if (Files.exists(current.resolve("build.mill"))) {
                return current.toAbsolutePath()
            }
            current = current.parent
        }
        throw IllegalStateException("Could not find project root. Are you running via Mill?")
    }

    fun getAssemblyJar(): Path {
        val jar = getProjectRoot().resolve("out/jdbcConnector/assembly.dest/out.jar")
        if (!Files.exists(jar) || Files.size(jar) == 0L) {
            throw IllegalStateException("Assembly JAR not found or empty at $jar. Run './mill jdbcConnector.assembly' first.")
        }
        return jar
    }

}
addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.8.0")

exportJars := true

Compile /mainClass := Some("Main")


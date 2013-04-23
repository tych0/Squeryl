if test -f .sbtconfig; then
  . .sbtconfig
fi
exec java ${SBT_OPTS} -XX:PermSize=256M -Xmx2048M -jar sbt-launch.jar "$@"

export FLAGS="-Djavax.net.ssl.trustStore=/usr/local/admin/uat2_certs/gdch01-cluster02.truststore"

export OOZIE_URL=https://gdch01d13.statestr.com:11443/oozie

oozie $FLAGS job $OOZIE_URL -config ../properties/file_data-dev.properties -run


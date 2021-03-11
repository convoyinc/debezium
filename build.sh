docker build --iidfile docker_image_id.txt -f ./build_images/jdk_11_0_10/Dockerfile .
image_id = $(cat docker_image_id.txt)
container_id=$(docker create image-name)

rm -rf ./build_images/jdk_11_0_10/output
mkdir -p ./build_images/jdk_11_0_10/output
docker cp $id:/build_images/debezium-connector-postgres/target/debezium-connector-postgres-0.9.5.1.Final.jar - > ./build_images/jdk_11_0_10/output/debezium-connector-postgres-0.9.5.1.Final.jar
docker cp $id:/build_images/debezium-core/target/debezium-core-0.9.5.1.Final.jar - > ./build_images/jdk_11_0_10/output/debezium-core-0.9.5.1.Final.jar

curl https://repo1.maven.org/maven2/org/postgresql/postgresql/42.2.5/postgresql-42.2.5.jar postgresql-42.2.5.jar -o ./build_images/jdk_11_0_10/output/postgresql-42.2.5.jar 
curl https://repo1.maven.org/maven2/com/google/protobuf/protobuf-java/2.6.1/protobuf-java-2.6.1.jar -o ./build_images/jdk_11_0_10/output/protobuf-java-2.6.1.jar 

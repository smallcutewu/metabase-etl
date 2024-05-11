FROM swr.cn-east-3.myhuaweicloud.com/release/openjdk:17-centos

ENV TZ=CST-8

RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone



ADD boulderai-etl-starter/target/boulderai-etl-starter-*.jar /app.jar

ENTRYPOINT ["sh","-c","java -jar -Dfile.encoding=UTF-8 -XX:+UnlockExperimentalVMOptions    /app.jar "]

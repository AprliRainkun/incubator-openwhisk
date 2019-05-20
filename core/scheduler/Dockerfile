# Licensed to the Apache Software Foundation (ASF) under one or more contributor
# license agreements; and to You under the Apache License, Version 2.0.

FROM scala

ENV UID=1001 \
    NOT_ROOT_USER=owuser

RUN apk add --update openssl

# copy app jars
ADD build/distributions/scheduler.tar /

COPY init.sh /
RUN chmod +x init.sh

RUN adduser -D -u ${UID} -h /home/${NOT_ROOT_USER} -s /bin/bash ${NOT_ROOT_USER}

EXPOSE 8080
EXPOSE 8181

CMD ["./init.sh", "0"]
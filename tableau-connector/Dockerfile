FROM alpine:3.14
RUN apk add --no-cache python3 bash git openjdk11
WORKDIR /tableau-connector
ADD ./src .
WORKDIR /tableau-sdk
RUN git clone https://github.com/tableau/connector-plugin-sdk.git &&\
    cd ./connector-plugin-sdk/connector-packager &&\
    python3 -m venv .venv &&\
    source ./.venv/bin/activate &&\
    python3 setup.py install &&\
    python3 -m connector_packager.package /tableau-connector
ENTRYPOINT ["bash"]

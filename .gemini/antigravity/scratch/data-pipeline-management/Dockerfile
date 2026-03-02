FROM 728921286627.dkr.ecr.us-east-1.amazonaws.com/golden/cicdbuilder:2.1.0-multi-arch AS builder
SHELL ["/bin/bash", "-c"]
ARG PIP_INDEX_URL
ARG INDATA_AWS_ACCESS_KEY_ID=${INDATA_AWS_ACCESS_KEY_ID}
ARG INDATA_AWS_SECRET_ACCESS_KEY=${INDATA_AWS_SECRET_ACCESS_KEY}
ENV ENABLE_THIRD_PARTY_LOG=True

COPY pipeline_version .
COPY pipeline_fetch.sh .
RUN ./pipeline_fetch.sh


FROM 728921286627.dkr.ecr.us-east-1.amazonaws.com/golden/dpm:v8-multi-arch
USER root
ARG PIP_INDEX_URL
ARG PRIVATE_PIP_PASSWORD=${PRIVATE_PIP_PASSWORD}
ARG PRIVATE_PIP_USERNAME=${PRIVATE_PIP_USERNAME}
ENV ENABLE_THIRD_PARTY_LOG=True

WORKDIR /opt/packages
COPY setup_env.sh .

RUN mkdir -p /packages/jars/latest
RUN mkdir -p /packages/jars/dependencies
RUN mkdir -p /opt/packages/client-module/keysets/keyset1
RUN mkdir -p /opt/packages/client-module/keysets/keyset2

# ClientPipeline
COPY --from=builder modules/client-module/keysets/keyset1/ /opt/packages/client-module/keysets/keyset1/
COPY --from=builder modules/client-module/keysets/keyset2/ /opt/packages/client-module/keysets/keyset2/
COPY --from=builder jars/clientpipeline/ /packages/jars/

# ProcessingPipeline
#COPY --from=builder jars/processingpipeline/ /packages/jars/latest/

# WritePipeline
#COPY --from=builder jars/writepipeline/writePipeline.jar /packages/jars/latest/writePipeline.jar

# IngestionPipeline
#COPY --from=builder jars/ingestionpipeline/ /packages/jars/

# ParsingPipeline
#COPY --from=builder jars/parsingpipeline/ /packages/jars/

# InferencePipeline
#COPY --from=builder jars/inferencepipeline/ /packages/jars/

# CCLFParser
COPY --from=builder jars/dependencies/ /packages/jars/dependencies/
#COPY --from=builder jars/cclfparser/ /packages/jars/latest/

# CCDAParser
#COPY --from=builder jars/ccdaparser/ /packages/jars/latest/

# APIPipeline
COPY --from=builder jars/apipipeline/ /packages/jars/latest/

# PythonPipeline
#COPY --from=builder jars/pythonpipeline/ /packages/jars/latest/

# WorkflowTrigger
COPY --from=builder jars/workflowtrigger/ /packages/jars/latest/

# Datalake to PG
ADD datalake_to_pg/ /packages/jars/latest/

# PG to Datalake
ADD pg_to_datalake/ /packages/jars/latest/


# PipelineExecutor
RUN mkdir /packages/jars/latest/pipeline-executor
ADD pipeline-executor/ /packages/jars/latest/pipeline-executor/
ADD pipeline_version /packages/jars/
RUN ln -sfn /usr/bin/python3.9 /usr/bin/python
RUN pip3 install -U pip
RUN python3 -m pip install pymongo==3.12.3
RUN python3 -m pip install requests==2.23.0
RUN python3 -m pip install smart_open==7.0.4
RUN python3 -m pip install azure-storage-file-datalake==12.1.2
RUN python3 -m pip install azure-identity==1.4.0
RUN python3 -m pip install virtualenv==16.7.9
RUN python3 -m pip install measure_output_validation_utility==1.3.8
RUN python3 -m pip install py-json-logger==0.2.11 --extra-index-url https://${ARTIFACT_USERNAME}:${ARTIFACT_PASSWORD}@pypi-domain-728921286627.d.codeartifact.us-east-1.amazonaws.com/pypi/pypi-repository/simple
RUN python3 -m pip install init-checks --extra-index-url https://${ARTIFACT_USERNAME}:${ARTIFACT_PASSWORD}@pypi-domain-728921286627.d.codeartifact.us-east-1.amazonaws.com/pypi/pypi-repository/simple    
RUN python3 -m pip install execpy==11.1.0 --extra-index-url https://${PRIVATE_PIP_USERNAME}:${PRIVATE_PIP_PASSWORD}@artifactory.innovaccer.net/repository/execpy/simple --extra-index-url https://${PRIVATE_PIP_USERNAME}:${PRIVATE_PIP_PASSWORD}@artifactory.innovaccer.net/repository/py-engineering/simple
RUN python3 -m pip install dqcc-l3-l5==0.0.1 --extra-index-url https://${PRIVATE_PIP_USERNAME}:${PRIVATE_PIP_PASSWORD}@artifactory.innovaccer.net/repository/dqcc-l3-l5/simple --use-deprecated=legacy-resolver
RUN python3 -m pip install dqcc-l2-l3==0.0.1 --extra-index-url https://${PRIVATE_PIP_USERNAME}:${PRIVATE_PIP_PASSWORD}@artifactory.innovaccer.net/repository/dqcc-l2-l3/simple --use-deprecated=legacy-resolver
RUN python3 -m pip install dqcc-l1-l2==0.0.1 --extra-index-url https://${PRIVATE_PIP_USERNAME}:${PRIVATE_PIP_PASSWORD}@artifactory.innovaccer.net/repository/dqcc-l1-l2/simple --use-deprecated=legacy-resolver
RUN python3 -m pip install dqcc-l0-l1==0.0.1 --extra-index-url https://${PRIVATE_PIP_USERNAME}:${PRIVATE_PIP_PASSWORD}@artifactory.innovaccer.net/repository/dqcc-l0-l1/simple --use-deprecated=legacy-resolver


#Template mssp
RUN python3 -m pip install template_mssp==1.1.14 --extra-index-url https://${PRIVATE_PIP_USERNAME}:${PRIVATE_PIP_PASSWORD}@artifactory.innovaccer.net/repository/template_mssp/simple --use-deprecated=legacy-resolver
RUN python3 -m pip install pandas==1.5.3
RUN python3 -m pip install lxml
RUN python3 -m pip install azure-servicebus==7.0.1
RUN python3 -m pip install psycopg2-binary==2.8.6
RUN python3 -m pip install sqlalchemy==1.4.25
RUN python3 -m pip install snowflake-sqlalchemy==1.2.4
RUN python3 -m pip install sqlalchemy-redshift==0.8.6
RUN python3 -m pip install snowflake-connector-python==2.9.0
RUN python3 -m pip install pyarrow==4.0.0
RUN python3 -m pip install openpyxl==3.0.10
RUN python3 -m pip install PyMySQL==1.0.2
RUN python3 -m pip install elasticsearch==5.5.3
RUN python3 -m pip install httplib2==0.20.4
RUN python3 -m pip install tabulate==0.8.10
RUN python3 -m pip install pgpy==0.6.0
RUN python3 -m pip install python-gnupg==0.5.2

#Pathlib for FHIR node
RUN python3 -m pip install pathlib



# FHIR Validator
RUN python3 -m pip install fhir-validator==0.1.6
# FHIR Shepherd
RUN python3 -m pip install fhir-shepherd==1.0.1

# FHIR NODE 1: pushtowarehouse
RUN python3 -m pip install pushtowarehouse==0.1.2

# FHIR NODE 2: l1_ingestion
RUN python3 -m pip install l1_ingestion==0.1.2

# FHIR NODE 3: fhir_parser_master
RUN python3 -m pip install fhir_parser_master==1.0.2




# Member Roster
COPY --from=builder jars/member-roster-extraction-pipeline/ /packages/jars/latest/
# RUN python3 -m pip install msal==1.4.3
RUN python3 -m pip install psycopg2-binary==2.8.6
RUN python3 -m pip install python-dotenv==0.14.0
RUN python3 -m pip install urllib3==1.26.5
RUN python3 -m pip install numpy==1.22.0
RUN python3 -m pip install xlrd==2.0.1
RUN python3 -m pip install s3fs
RUN python3 -m pip install faker
RUN python3 -m pip install pg8000==1.15.2

#PD Aidbox pipeline
RUN python3 -m pip install pdl2dappipeline==1.1.0rc2

# Provider Roster
COPY --from=builder jars/provider-roster-extraction-pipeline/ /packages/jars/latest/

# Utility Pipeline
#COPY --from=builder jars/utilitypipeline/ /packages/jars/latest/

RUN mkdir -p /opt/packages/mongo_prerequisites
RUN mkdir -p /opt/packages/mysql_prerequisites
ADD mongo_prerequisites/ /opt/packages/mongo_prerequisites
ADD mysql_prerequisites/ /opt/packages/mysql_prerequisites
ADD schedule_prerequisites/ /opt/packages/schedule_prerequisites

RUN pip install python-dotenv pymongo mysql-connector-python==8.0.29

WORKDIR /opt/packages
COPY INIT_CHECKS.yaml .
ADD https://artifacts.elastic.co/downloads/beats/filebeat/filebeat-8.1.1-linux-x86_64.tar.gz ./
RUN tar -xf filebeat-8.1.1-linux-x86_64.tar.gz && rm filebeat-8.1.1-linux-x86_64.tar.gz
COPY entrypoint.sh .

RUN apt remove curl -y
RUN apt autoclean && apt autoremove -y
RUN apt-get update -y
RUN apt-get install -y libc6-dev

RUN dpkg -r --force-all apt apt-get && dpkg -r --force-all debconf dpkg
#RUN groupadd -g 999 appuser && \
#  useradd -r -u 999 -g  appuser -m appuser

# change the uid of new_user to ensure it has whatever it was assigned plus 1 (e.g. if UID was 1000, now it'll be 1001)
#RUN id -u appuser | awk '{print $1+1}' | xargs -I{} usermod -u {} appuser

RUN id -u appuser | xargs -I{} chown -R {}:{} /home
RUN id -u appuser | xargs -I{} chown -R {}:{} /opt
RUN id -u appuser | xargs -I{} chown -R {}:{} /var
RUN id -u appuser | xargs -I{} chown -R {}:{} /azkaban
RUN id -u appuser | xargs -I{} chown -R {}:{} /packages

USER appuser

ENTRYPOINT ["sh","./entrypoint.sh"]


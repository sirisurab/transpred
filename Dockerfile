FROM sirisurab/tp-app-pkgs AS app

#RUN GIT_URL="https://github.com/sirisurab/transpred/archive/master.zip" && \
#wget --no-check-certificate -O master.zip $GIT_URL && \
ARG SSH_PRIVATE_KEY
ARG SSH_PUB_KEY
RUN rm -Rf /app && mkdir /app && \
mkdir -p $HOME/.ssh && \
echo "${SSH_PRIVATE_KEY}" > $HOME/.ssh/id_rsa && \
echo "${SSH_PUB_KEY}" > $HOME/.ssh/id_rsa.pub && \
chmod -R 400 $HOME/.ssh/ && \
ssh-keyscan github.com > $HOME/.ssh/known_hosts && \
git clone -v "ssh://git@github.com/sirisurab/transpred.git" /app
#unzip master.zip && \
#mv /app/transpred-master/* /app && \
#chmod -R +x /app && \
#rm -r /app/transpred-master && \
#rm master.zip

# Set the working directory to /app
WORKDIR /app
#RUN chmod -R 777 /app
# Make port 80 available to the world outside this container
#EXPOSE 80

# Define environment variable
# ENV NAME World
#VOLUME /app
ENTRYPOINT ["/bin/bash"]
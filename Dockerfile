FROM sirisurab/tp-app-pkgs AS app

# Set the working directory to /app
WORKDIR /app

RUN GIT_URL="https://github.com/sirisurab/transpred/archive/master.zip" && \
wget --no-check-certificate -O master.zip $GIT_URL && \
#git clone $GIT_URL && cd src && git checkout 8054d2db3cd16d6862bb140adda7060b4dbbc5cc
unzip master.zip && \
mv /app/transpred-master/* /app && \
chmod -R +x /app && \
rm -r /app/transpred-master && \
rm master.zip


#RUN chmod -R 777 /app
# Make port 80 available to the world outside this container
#EXPOSE 80

# Define environment variable
# ENV NAME World
#VOLUME /app
ENTRYPOINT ["/bin/bash"]
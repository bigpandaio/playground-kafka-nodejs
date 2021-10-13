FROM node:15.6.0-alpine3.12

WORKDIR /opt/app/

COPY ./deploy/dist /opt/app/dist
COPY package.json /opt/app/dist
COPY package-lock.json /opt/app/dist
RUN (cd /opt/app/dist && npm install --production)

ENV config_env=prod

CMD node /opt/app/dist/server.js


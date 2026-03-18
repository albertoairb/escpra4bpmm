FROM node:20-alpine

WORKDIR /app

COPY package*.json ./
RUN npm install --omit=dev

COPY server.js ./server.js
COPY public ./public
COPY schema.sql ./schema.sql

ENV NODE_ENV=production
EXPOSE 8080

CMD ["npm","start"]


FROM node:20-alpine

WORKDIR /app

COPY package*.json ./
RUN npm install --omit=dev

COPY index.js ./
COPY log-reconciliation.js ./

CMD ["node", "index.js"]

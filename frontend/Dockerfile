FROM node:18-alpine AS build
WORKDIR /FD-finder/frontend
COPY package*.json ./
COPY .env ./
RUN npm install
COPY . .
RUN npm run build

# Production Stage
FROM nginx:stable-alpine AS production
COPY --from=build /FD-finder/frontend/dist /usr/share/nginx/html
COPY --from=build /FD-finder/frontend/nginx/nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]

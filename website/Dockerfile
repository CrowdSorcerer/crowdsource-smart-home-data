FROM node

RUN git clone https://github.com/CrowdSorcerer/crowdsource-smart-home-site.git

WORKDIR /crowdsource-smart-home-site

RUN npm install 

RUN npm run build

CMD ["npm", "start", "--", "--port", "80"]
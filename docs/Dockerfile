# Use the official Node.js image as the base image
FROM node:22-alpine@sha256:968df39aedcea65eeb078fb336ed7191baf48f972b4479711397108be0966920

# Install pnpm
RUN npm install -g pnpm@11.0.9

# Set the working directory to /app
WORKDIR /app

# Copy the package.json and pnpm-lock.yaml files to the working directory
COPY package.json pnpm-lock.yaml ./

# Install dependencies using pnpm
RUN pnpm install --ignore-scripts

# Copy the rest of your application code to the working directory
COPY . .

# Build the Next.js application
RUN pnpm build

# Expose port 3000 to the outside world
EXPOSE 3000

# Command to run the Next.js application
CMD ["pnpm", "start"]

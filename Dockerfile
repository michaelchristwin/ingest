FROM oven/bun:latest

WORKDIR /src

# Copy package files
COPY package.json bun.lockb* ./

# Install dependencies
RUN bun install --production

# Copy app source
COPY . .

# Expose port your app listens on
EXPOSE 3001

# Command to start your Elysia app
CMD ["bun", "run", "start"]

FROM ghcr.io/astral-sh/uv:python3.13-trixie

# Set the default working directory inside the image.
# All later COPY, RUN, and CMD instructions will work relative to /app.
WORKDIR /app

# Tell uv not to install development dependencies.
# This keeps the image closer to a deployment build.
ENV UV_NO_DEV=1

# Copy dependency and project metadata first.
# Doing this before copying source code helps Docker cache the dependency layer.
COPY pyproject.toml uv.lock README.md ./

# Install project dependencies first, but not the project source itself yet.
# --locked uses the lockfile exactly.
# --no-install-project skips installing the app package at this stage.
# --no-editable avoids editable installs in the container.
RUN uv sync --locked --no-install-project --no-editable

# Copy the application package while preserving the app/ directory.
# This is the critical fix. We want /app/app/... inside the container.
COPY app ./app

# Now install/sync the project itself after the source code exists in the image.
RUN uv sync --locked --no-editable

# Document that the application listens on port 8000 inside the container.
EXPOSE 8000

# Start the FastAPI app with Uvicorn.
# --host 0.0.0.0 is required so the app is reachable from outside the container.
# --port 8000 matches the exposed port.
CMD ["uv", "run", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]

CI_PROJECT_ID=74704703
PRIVATE_TOKEN=""
CI_COMMIT_TAG="v0.0.1d"
IMAGE_TAG="v0.0.1d"

docker build --pull -f Dockerfile_CI --build-arg GITLAB_PROJECT_ID="$CI_PROJECT_ID" --build-arg PRIVATE_TOKEN="$PRIVATE_TOKEN" --build-arg CI_COMMIT_TAG="$CI_COMMIT_TAG" -t "$IMAGE_TAG" .


#

docker login registry.gitlab.com -u xmival00 -p $PRIVATE_TOKEN
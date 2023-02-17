
7. Build the images and push to registry ( this takes around 1.5 hours for the first time )



bazel query 'filter("push_kingdom", kind("container_push", //src/main/docker:all))' |
 xargs bazel build -c opt --define container_registry=gcr.io \
 --define image_repo_prefix=halo-kingdom-demo-368110 --define image_tag=build-0002
bazel query 'filter("push_kingdom", kind("container_push", //src/main/docker:all))' |
 xargs -n 1 bazel run -c opt --define container_registry=gcr.io \
 --define image_repo_prefix=halo-kingdom-demo-368110 --define image_tag=build-0002
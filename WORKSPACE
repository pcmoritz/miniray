load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive", "http_file")

http_archive(
    name = "com_google_absl",
    url = "https://github.com/abseil/abseil-cpp/archive/refs/tags/20210324.2.tar.gz",
    strip_prefix = "abseil-cpp-20210324.2",
    sha256 = "59b862f50e710277f8ede96f083a5bb8d7c9595376146838b9580be90374ee1f",
)

http_archive(
    name = "com_github_grpc_grpc",
    url = "https://github.com/grpc/grpc/archive/refs/tags/v1.38.1.tar.gz",
    strip_prefix = "grpc-1.38.1",
    sha256 = "f60e5b112913bf776a22c16a3053cc02cf55e60bf27a959fd54d7aaf8e2da6e8",
)

load("@com_github_grpc_grpc//bazel:grpc_deps.bzl", "grpc_deps")

grpc_deps()


load("@com_github_grpc_grpc//third_party/py:python_configure.bzl", "python_configure")
python_configure(name="local_config_python")

load("@com_github_grpc_grpc//bazel:grpc_extra_deps.bzl", "grpc_extra_deps")
grpc_extra_deps()
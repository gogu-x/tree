# 生成器只在显式 tools-build 时编译；生成命令复用已编译工具。
GEN_DIR ?= task
STRUCTS ?= Instance State Event Change
.PHONY: tools-build accessor deepcopy
tools-build:
	go build -o tools/accessor_gen/accessor-gen ./tools/accessor_gen
	go build -o tools/deepcopy-gen/deepcopy-gen ./tools/deepcopy-gen
accessor:
	./tools/accessor_gen/accessor-gen -dir "$(GEN_DIR)" $(STRUCTS)
deepcopy:
	./tools/deepcopy-gen/deepcopy-gen -dir "$(GEN_DIR)"

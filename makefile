SHELL = /bin/bash

project_dependencies ?= $(addprefix $(project_root)/, cltl-knowledgerepresentation emissor cltl-combot cltl-requirements)

git_remote ?= https://github.com/leolani

artifact_name = cltl.entity_linking


include util/make/makefile.base.mk
include util/make/makefile.component.mk
include util/make/makefile.py.base.mk
include util/make/makefile.git.mk

docker:
	$(info "No docker build for $(project_name)")

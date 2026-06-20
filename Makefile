OPENRESTY_PREFIX=/opt/openresty

PREFIX ?=          /usr/local
LUA_INCLUDE_DIR ?= $(PREFIX)/include
LUA_LIB_DIR ?=     $(PREFIX)/lib/lua/$(LUA_VERSION)
INSTALL ?= install

.PHONY: all install

all: ;

install: all
	$(INSTALL) -d $(DESTDIR)$(LUA_LIB_DIR)/resty
	$(INSTALL) lib/resty/*.lua $(DESTDIR)$(LUA_LIB_DIR)/resty
	$(INSTALL) -d $(DESTDIR)$(LUA_LIB_DIR)/resty/redis_mux
	$(INSTALL) lib/resty/redis_mux/*.lua $(DESTDIR)$(LUA_LIB_DIR)/resty/redis_mux

PERL5LIB ?= $(HOME)/perl5/lib/perl5

test: all
	PERL5LIB="$(PERL5LIB)" PATH=$(OPENRESTY_PREFIX)/nginx/sbin:$$PATH prove -I. -I../test-nginx/lib -r t

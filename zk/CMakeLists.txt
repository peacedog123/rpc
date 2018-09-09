SET(SRC_LIST subscriber.cc register.cc util.cc)

add_definitions(-DTHREADED)

set(SERVICE_LIBS
    ${ZOOKEEPER_STATIC_LIB}
    pthread
)

ADD_EXPORTABLE_LIBRARY(service
  SRCS ${SRC_LIST} ${PROTO_SRCS}
  DEPS ${SERVICE_LIBS}
)

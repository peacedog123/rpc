set(KRPC_SRCS
    connection.cc
    connection_id.cc
    constants.cc
    messenger.cc
    negotiation.cc
    outbound_call.cc
    proxy.cc
    reactor.cc
    remote_method.cc
    rpc_controller.cc
    rpc_sidecar.cc
    transfer.cc
    )

set(KRPC_LIBS
  gutil
  kudu_util
  ev)

ADD_EXPORTABLE_LIBRARY(krpc
  SRCS ${KRPC_SRCS} ${PROTO_SRCS}
  DEPS ${KRPC_LIBS})

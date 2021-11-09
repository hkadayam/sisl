#include "grpc_helper/rpc_client.hpp"
#include "utils.hpp"

namespace grpc_helper {

GrpcBaseClient::GrpcBaseClient(const std::string& server_addr, const std::string& target_domain,
                               const std::string& ssl_cert) :
        m_server_addr(server_addr), m_target_domain(target_domain), m_ssl_cert(ssl_cert) {}

void GrpcBaseClient::init() {
    ::grpc::SslCredentialsOptions ssl_opts;
    if (!m_ssl_cert.empty()) {
        if (load_ssl_cert(m_ssl_cert, ssl_opts.pem_root_certs)) {
            if (!m_target_domain.empty()) {
                ::grpc::ChannelArguments channel_args;
                channel_args.SetSslTargetNameOverride(m_target_domain);
                m_channel = ::grpc::CreateCustomChannel(m_server_addr, ::grpc::SslCredentials(ssl_opts), channel_args);
            } else {
                m_channel = ::grpc::CreateChannel(m_server_addr, ::grpc::SslCredentials(ssl_opts));
            }

        } else {
            throw std::runtime_error("Unable to load ssl certification for grpc client");
        }
    } else {
        m_channel = ::grpc::CreateChannel(m_server_addr, ::grpc::InsecureChannelCredentials());
    }
}

bool GrpcBaseClient::load_ssl_cert(const std::string& ssl_cert, std::string& content) {
    return get_file_contents(ssl_cert, content);
}

bool GrpcBaseClient::is_connection_ready() const {
    return (m_channel->GetState(true) == grpc_connectivity_state::GRPC_CHANNEL_READY);
}

std::mutex GrpcAsyncClientWorker::s_workers_mtx;
std::unordered_map< std::string, GrpcAsyncClientWorker::UPtr > GrpcAsyncClientWorker::s_workers;

GrpcAsyncClientWorker::~GrpcAsyncClientWorker() { shutdown(); }
void GrpcAsyncClientWorker::shutdown() {
    if (m_state == ClientState::RUNNING) {
        m_cq.Shutdown();
        m_state = ClientState::SHUTTING_DOWN;

        for (auto& thr : m_threads) {
            thr.join();
        }

        m_state = ClientState::TERMINATED;
    }

    return;
}

void GrpcAsyncClientWorker::run(uint32_t num_threads) {
    LOGMSG_ASSERT_EQ(ClientState::INIT, m_state);

    if (num_threads == 0) { throw(std::invalid_argument("Need atleast one worker thread")); }
    for (uint32_t i = 0u; i < num_threads; ++i) {
        m_threads.emplace_back(&GrpcAsyncClientWorker::client_loop, this);
    }

    m_state = ClientState::RUNNING;
}

void GrpcAsyncClientWorker::client_loop() {
#ifdef _POSIX_THREADS
#ifndef __APPLE__
    auto tname = std::string("grpc_client").substr(0, 15);
    pthread_setname_np(pthread_self(), tname.c_str());
#endif /* __APPLE__ */
#endif /* _POSIX_THREADS */

    void* tag;
    bool ok = false;
    while (m_cq.Next(&tag, &ok)) {
        // For client-side unary call, `ok` is always true, even server is not running
        auto cm = static_cast< ClientRpcDataAbstract* >(tag);
        cm->handle_response(ok);
        delete cm;
    }
}

void GrpcAsyncClientWorker::create_worker(const std::string& name, int num_threads) {
    std::lock_guard< std::mutex > lock(s_workers_mtx);
    if (s_workers.find(name) != s_workers.end()) { return; }

    auto worker = std::make_unique< GrpcAsyncClientWorker >();
    worker->run(num_threads);
    s_workers.insert(std::make_pair(name, std::move(worker)));
}

GrpcAsyncClientWorker* GrpcAsyncClientWorker::get_worker(const std::string& name) {
    std::lock_guard< std::mutex > lock(s_workers_mtx);
    auto it = s_workers.find(name);
    if (it == s_workers.end()) { return nullptr; }
    return it->second.get();
}

void GrpcAsyncClientWorker::shutdown_all() {
    std::lock_guard< std::mutex > lock(s_workers_mtx);
    for (auto& it : s_workers) {
        it.second->shutdown();
        // release worker, the completion queue holds by it need to  be destroyed before grpc lib internal object
        // g_core_codegen_interface
        it.second.reset();
    }
}
} // namespace grpc_helper

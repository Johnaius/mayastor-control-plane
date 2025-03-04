#![warn(missing_docs)]
//! Control Plane Agents library with emphasis on the rpc transport interaction
//! including errors.
//!
//! It's meant to facilitate the creation of agents with a helper builder to
//! subscribe handlers for different message identifiers.

use futures::Future;
use grpc::tracing::OpenTelServer;
use snafu::Snafu;
use state::TypeMap;
use std::{net::SocketAddr, path::Path, sync::Arc};
use stor_port::transport_api::ErrorChain;
use tonic::transport::ServerTlsConfig;

mod common;
/// Event definitions.
pub mod eventing;

/// Agent level errors.
pub use common::errors;

#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum ServiceError {
    #[snafu(display("GrpcServer error"))]
    GrpcServer { source: tonic::transport::Error },
    #[snafu(display("TLS error: {}", source))]
    TlsError { source: std::io::Error },
}

type LayerStack = tower::layer::util::Stack<OpenTelServer, tower::layer::util::Identity>;
/// An agent service with shareable state and a tonic server for gRPC services.
pub struct Service<S = tonic::transport::server::Router<LayerStack>> {
    shared_state: Arc<TypeMap![Send + Sync]>,
    tonic_server: S,
}
/// A `Service` that has not yet been added any routes.
pub type ServiceEmpty = Service<tonic::transport::Server<LayerStack>>;

impl ServiceEmpty {
    /// Create a router with the `S` typed service as the first service.
    ///
    /// This will clone the `Server` builder and create a router that will
    /// route around different services.
    #[must_use]
    pub fn with_service<S>(mut self, svc: S) -> Service
    where
        S: tower::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<tonic::body::BoxBody>,
                Error = std::convert::Infallible,
            > + tonic::server::NamedService
            + Clone
            + Send
            + 'static,
        S::Future: Send + 'static,
    {
        Service {
            shared_state: self.shared_state,
            tonic_server: self.tonic_server.add_service(svc),
        }
    }

    /// Add TLS configuration to the service.
    /// Must be called before adding any services.
    ///
    /// # Arguments
    /// * `tls_config` - The TLS configuration to apply
    ///
    /// # Returns
    /// * `Result<Self, ServiceError>` - The service with TLS configured
    ///
    /// # Example
    /// ```
    /// let service = Service::builder()
    ///     .with_tls(tls_config)?
    ///     .with_shared_state(state)
    ///     .configure(my_config);
    /// ```
    pub fn with_tls(self, tls_config: Option<ServerTlsConfig>) -> Result<Self, ServiceError> {
        let tonic_server = self
            .tonic_server
            .tls_config(tls_config.expect("TLS is not configured"))
            .map_err(|e| ServiceError::TlsError {
                source: std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to configure TLS: {}", e),
                ),
            })?;

        Ok(Self {
            shared_state: self.shared_state,
            tonic_server,
        })
    }
}

impl Service {
    /// Setup default service with an opentelemetry layer configured on the tonic server.
    pub fn builder() -> Service<tonic::transport::Server<LayerStack>> {
        Service::<tonic::transport::Server<LayerStack>> {
            shared_state: Arc::new(<TypeMap![Send + Sync]>::new()),
            tonic_server: tonic::transport::Server::builder().layer(OpenTelServer::new(vec![
                // This is a bit of hack, but tonic doesn't seem to provide access to this uri
                // path in any way.
                // todo: add ignored routes via shared state
                "/mayastor.v1.Registration/Register",
            ])),
        }
    }
    /// Configure TLS for the service using certificate and key files.
    /// # Arguments
    /// * `cert_path` - Optional path to the TLS certificate file
    /// * `key_path` - Optional path to the TLS private key file
    /// # Returns
    /// * `Ok(Some(ServerTlsConfig))` - When TLS is successfully configured
    /// * `Ok(None)` - When no TLS configuration is requested
    /// * `Err(ServiceError)` - If TLS configuration fails
    /// # Errors
    /// Returns a `ServiceError` if:
    /// - Only one of cert/key is provided
    /// - Certificate or key files cannot be read
    /// - TLS configuration is invalid
    pub fn configure_tls<P: AsRef<Path>>(
        cert_path: Option<P>,
        key_path: Option<P>,
    ) -> Result<Option<ServerTlsConfig>, ServiceError> {
        match (cert_path, key_path) {
            (Some(cert), Some(key)) => {
                // Read certificate file
                let cert_data = std::fs::read(cert.as_ref())
                    .map_err(|source| ServiceError::TlsError { source })?;
                // Read key file
                let key_data = std::fs::read(key.as_ref())
                    .map_err(|source| ServiceError::TlsError { source })?;
                // Create TLS config
                let identity = tonic::transport::Identity::from_pem(cert_data, key_data);
                let tls_config = ServerTlsConfig::new().identity(identity);

                Ok(Some(tls_config))
            }
            //if no cert or key provide erorr message that cert and key are required
            (None, _) | (_, None) => {
                tracing::warn!("TLS is not configured, both cert and key are required");
                Ok(None)
            }
        }
    }

    /// Adds a new service to the tonic server router.
    #[must_use]
    pub fn with_service<S>(self, svc: S) -> Self
    where
        S: tower::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<tonic::body::BoxBody>,
                Error = std::convert::Infallible,
            > + tonic::server::NamedService
            + Clone
            + Send
            + 'static,
        S::Future: Send + 'static,
    {
        Self {
            shared_state: self.shared_state,
            tonic_server: self.tonic_server.add_service(svc),
        }
    }

    /// Runs this server as a future until a shutdown signal is received.
    pub async fn run(self, socket: SocketAddr) {
        if let Err(error) = self.run_err(socket).await {
            tracing::error!(error = error.full_string(), "Error running service thread");
        }
    }

    /// Runs this server as a future until a shutdown signal is received.
    pub async fn run_err(self, socket: SocketAddr) -> Result<(), ServiceError> {
        self.tonic_server
            .serve_with_shutdown(socket, Self::shutdown_signal())
            .await
            .map_err(|source| ServiceError::GrpcServer { source })
    }

    /// Waits until the process receives a shutdown: either TERM or INT.
    /// The opentel traces are also immediately flushed before returning.
    pub async fn shutdown_signal() {
        shutdown::Shutdown::wait().await;
        utils::tracing_telemetry::flush_traces();
    }
}

impl<L> Service<L> {
    /// Add a new service-wide shared state which can be retried in the handlers
    /// (more than one type of data can be added).
    /// The type must be `Send + Sync + 'static`.
    ///
    /// Example:
    /// # async fn main() {
    /// Service::builder()
    ///         .with_shared_state(NodeStore::default())
    ///         .with_shared_state(More {})
    ///         .configure(configure)
    ///         .run().await;
    ///
    /// # async fn configure(builder: Service) -> Service {
    /// #  builder
    /// # }
    #[must_use]
    pub fn with_shared_state<T: Send + Sync + 'static>(self, state: T) -> Self {
        let type_name = std::any::type_name::<T>();
        tracing::debug!("Adding shared type: {}", type_name);
        if !self.shared_state.set(state) {
            panic!("Shared state for type '{type_name}' has already been set!");
        }
        self
    }
    /// Get the shared state of type `T` added with `with_shared_state`.
    pub fn shared_state<T: Send + Sync + 'static>(&self) -> &T {
        match self.shared_state.try_get() {
            Some(state) => state,
            None => {
                let type_name = std::any::type_name::<T>();
                let error_msg =
                    format!("Requested data type '{type_name}' not shared via with_shared_data");
                panic!("{}", error_msg);
            }
        }
    }

    /// Configure `self` through a configure closure.
    #[must_use]
    pub fn configure<F>(self, configure: F) -> Service
    where
        F: FnOnce(Self) -> Service,
    {
        configure(self)
    }

    /// Configure `self` through an async configure closure.
    pub async fn configure_async<F, Fut>(self, configure: F) -> Service
    where
        F: FnOnce(Self) -> Fut,
        Fut: Future<Output = Service>,
    {
        configure(self).await
    }
}

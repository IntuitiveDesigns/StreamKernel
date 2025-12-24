package com.example.streamkernel.kafka.spi;

public final class PluginCatalog {

    public final ServicePluginRegistry<SourcePlugin> sources;
    public final ServicePluginRegistry<SinkPlugin> sinks;
    public final ServicePluginRegistry<TransformerPlugin> transformers;
    public final ServicePluginRegistry<CachePlugin> caches;
    public final ServicePluginRegistry<DlqSerializerPlugin> dlqSerializers;

    public PluginCatalog(ClassLoader cl) {
        this.sources = new ServicePluginRegistry<>(SourcePlugin.class, cl, SourcePlugin::id);
        this.sinks = new ServicePluginRegistry<>(SinkPlugin.class, cl, SinkPlugin::id);
        this.transformers = new ServicePluginRegistry<>(TransformerPlugin.class, cl, TransformerPlugin::id);
        this.caches = new ServicePluginRegistry<>(CachePlugin.class, cl, CachePlugin::id);

        this.dlqSerializers = new ServicePluginRegistry<>(DlqSerializerPlugin.class, cl, DlqSerializerPlugin::id);
    }
}

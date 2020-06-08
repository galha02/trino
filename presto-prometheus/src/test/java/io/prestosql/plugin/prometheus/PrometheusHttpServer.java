/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.plugin.prometheus;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.http.server.TheServlet;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.http.server.testing.TestingHttpServerModule;
import io.airlift.node.testing.TestingNodeModule;

import javax.servlet.Servlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.Map;

import static io.prestosql.plugin.prometheus.PrometheusClient.METRICS_ENDPOINT;

public class PrometheusHttpServer
{
    private final LifeCycleManager lifeCycleManager;
    private final URI baseUri;

    public PrometheusHttpServer()
            throws Exception
    {
        Bootstrap app = new Bootstrap(
                new TestingNodeModule(),
                new TestingHttpServerModule(),
                new PrometheusHttpServerModule());

        Injector injector = app
                .strictConfig()
                .doNotInitializeLogging()
                .initialize();

        lifeCycleManager = injector.getInstance(LifeCycleManager.class);
        baseUri = injector.getInstance(TestingHttpServer.class).getBaseUrl();
    }

    public void stop()
            throws Exception
    {
        lifeCycleManager.stop();
    }

    public URI resolve(String s)
    {
        return baseUri.resolve(s);
    }

    private static class PrometheusHttpServerModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            binder.bind(new TypeLiteral<Map<String, String>>() {}).annotatedWith(TheServlet.class).toInstance(ImmutableMap.of());
            binder.bind(Servlet.class).annotatedWith(TheServlet.class).toInstance(new PrometheusHttpServlet());
        }
    }

    private static class PrometheusHttpServlet
            extends HttpServlet
    {
        @Override
        protected void doGet(HttpServletRequest request, HttpServletResponse response)
                throws IOException
        {
            URL dataUrl;
            // allow for special response on Prometheus metrics endpoint
            if (request.getPathInfo().contains(METRICS_ENDPOINT)) {
                dataUrl = Resources.getResource(TestPrometheusClient.class, request.getPathInfo().split(METRICS_ENDPOINT)[0]);
            }
            else {
                dataUrl = Resources.getResource(TestPrometheusClient.class, request.getPathInfo());
            }
            Resources.asByteSource(dataUrl).copyTo(response.getOutputStream());
        }
    }
}

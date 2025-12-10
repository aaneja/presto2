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
package com.facebook.presto.hive;

import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.google.common.collect.ImmutableList;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

public class HivePlugin
        implements Plugin
{
    private final String name;
    private final Optional<ExtendedHiveMetastore> metastore;

    public HivePlugin(String name)
    {
        this(name, Optional.empty());
    }

    public HivePlugin(String name, Optional<ExtendedHiveMetastore> metastore)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or empty");
        this.name = name;
        this.metastore = requireNonNull(metastore, "metastore is null");
    }

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(new HiveConnectorFactory(name, getClassLoader(), metastore));
    }

    private static ClassLoader getClassLoader()
    {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null) {
            classLoader = HivePlugin.class.getClassLoader();
        }

        // Print classpath for debugging
        System.out.println("=== Effective ClassLoader: " + classLoader + " ===");

        if (classLoader instanceof URLClassLoader) {
            URLClassLoader urlCl = (URLClassLoader) classLoader;
            System.out.println("Classpath entries:");
            for (URL url : urlCl.getURLs()) {
                System.out.println("  " + url);
            }
        }
        else {
            // Modern JVMs (Java 9+) use non-URL classloaders (BuiltinClassLoader)
            // They do not expose their classpath directly.
            System.out.println("Classpath entries not directly accessible from classLoader type: "
                    + classLoader.getClass().getName());

            // Fallback: print the *system* classpath
            System.out.println("java.class.path =");
            for (String cp : System.getProperty("java.class.path").split(File.pathSeparator)) {
                System.out.println("  " + cp);
            }
        }

        return classLoader;
    }
}

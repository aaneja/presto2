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
package com.facebook.presto.spi.router;
import javax.servlet.http.HttpServletRequest;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface Scheduler
{
    /**
     * Schedules a query from a user to a concrete candidate. Returns the
     * URI of this candidate.
     */
    Optional<URI> getDestination(String user);

    /**
     * Schedules a query from a user to a concrete candidate. Returns the
     * URI of this candidate.
     * TODO : Refactor to use a RequestInfo POJO and remove the extra getDestination variant
     */
    default Optional<URI> getDestination(String user, String query)
    {
        return Optional.empty();
    };

    /**
     * A `getDestination` variant that allows a custom scheduler to modify the inbound {@link RequestInfo} object
     * as it sees fit.
     * The URI routed to is obtained from the {@link RequestInfo#getServletRequest().getRequestURI()} object.
     *
     * As an example, the custom scheduler could modify the request headers such as PRESTO_CLIENT_TAGS and have the request
     * re-routed back to the router, so that the request can be routed around in a chained manner
     *
     * This was also a follow-up item from https://github.com/prestodb/presto/pull/24439#discussion_r2063267082
     */
    default Optional<RequestInfo> getDestination(RequestInfo inboundRequestInfo)
    {
        return Optional.empty();
    }

    /**
     * Sets the candidates with the list of URIs for scheduling.
     */
    void setCandidates(List<URI> candidates);

    /**
     * Sets remote cluster infos for each cluster.
     */
    default void setClusterInfos(Map<URI, ClusterInfo> clusterInfos) {}

    /**
     * Sets the candidate group name
     */
    default void setCandidateGroupName(String candidateGroupName) {}

    /**
     * Sets the weights of candidates with a hash map object.
     */
    default void setWeights(Map<URI, Integer> weights) {}

    default Optional<List<String>> getExtraClientTags(String statement, HttpServletRequest httpServletRequest) {
        return Optional.empty();
    }
}

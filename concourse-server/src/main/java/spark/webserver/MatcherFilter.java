/*
 * Copyright (c) 2013-2019 Cinchapi Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package spark.webserver;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;

import spark.Access;
import spark.HaltException;
import spark.Request;
import spark.RequestResponseFactory;
import spark.Response;
import spark.Route;
import spark.route.HttpMethod;
import spark.route.RouteMatch;
import spark.route.RouteMatcher;

import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.util.Logger;

/**
 * Filter for matching of filters and routes.
 *
 * @author Per Wendel
 */
public class MatcherFilter implements Filter {

    /**
     * Check the path of the {@code servletRequest} and return {@code true} of
     * that path is for a static file.
     * 
     * @param servletRequest
     * @param servletResponse
     * @param chain
     * @return {@code true} if the request is for a static file instead of a
     *         route.
     */
    private static boolean isStaticFileRequest(ServletRequest servletRequest,
            ServletResponse servletResponse, FilterChain chain) {
        String path = Reflection.get("_pathInfo", servletRequest);
        for (String ext : STATIC_FILE_EXTENSIONS) {
            if(path.endsWith(ext)) {
                return true;
            }
        }
        return false;
    }

    private static final String ACCEPT_TYPE_REQUEST_MIME_HEADER = "Accept";
    private static final String INTERNAL_ERROR = "<html><body><h2>500 Internal Error</h2></body></html>";
    private static final String NOT_FOUND = "<html><body><h2>404 Not found</h2>The requested route [%s] has not been mapped in Spark</body></html>";

    /**
     * Valid static file extensions.
     */
    private static final String[] STATIC_FILE_EXTENSIONS = { "jpg", "jpeg",
            "png", "css", "ico", "gif", "js" };

    private boolean hasOtherHandlers;

    private boolean isServletContext;

    private RouteMatcher routeMatcher;

    /**
     * Constructor
     *
     * @param routeMatcher The route matcher
     * @param isServletContext If true, chain.doFilter will be invoked if
     *            request is not consumed by Spark.
     * @param hasOtherHandlers If true, do nothing if request is not consumed by
     *            Spark in order to let others handlers process the request.
     */
    public MatcherFilter(RouteMatcher routeMatcher, boolean isServletContext,
            boolean hasOtherHandlers) {
        this.routeMatcher = routeMatcher;
        this.isServletContext = isServletContext;
        this.hasOtherHandlers = hasOtherHandlers;
    }

    public void destroy() {
        // TODO Auto-generated method stub
    }

    public void doFilter(ServletRequest servletRequest,
            ServletResponse servletResponse, // NOSONAR
            FilterChain chain) throws IOException, ServletException { // NOSONAR
        HttpServletRequest httpRequest = (HttpServletRequest) servletRequest; // NOSONAR
        HttpServletResponse httpResponse = (HttpServletResponse) servletResponse;

        String httpMethodStr = httpRequest.getMethod().toLowerCase(); // NOSONAR
        String uri = httpRequest.getRequestURI(); // NOSONAR
        String acceptType = httpRequest
                .getHeader(ACCEPT_TYPE_REQUEST_MIME_HEADER);

        String bodyContent = null;
        if(!isStaticFileRequest(servletRequest, servletResponse, chain)) {
            RequestWrapper req = new RequestWrapper();
            ResponseWrapper res = new ResponseWrapper();

            try {
                // BEFORE filters
                List<RouteMatch> matchSet = routeMatcher
                        .findTargetsForRequestedRoute(HttpMethod.before, uri,
                                acceptType);

                for (RouteMatch filterMatch : matchSet) {
                    Object filterTarget = filterMatch.getTarget();
                    if(filterTarget instanceof spark.Filter) {
                        Request request = RequestResponseFactory
                                .create(filterMatch, httpRequest);
                        Response response = RequestResponseFactory
                                .create(httpResponse);

                        spark.Filter filter = (spark.Filter) filterTarget;

                        req.setDelegate(request);
                        res.setDelegate(response);

                        filter.handle(req, res);

                        String bodyAfterFilter = Access.getBody(response);
                        if(bodyAfterFilter != null) {
                            bodyContent = bodyAfterFilter;
                        }
                    }
                }
                // BEFORE filters, END

                HttpMethod httpMethod = HttpMethod.valueOf(httpMethodStr);

                RouteMatch match = null;
                match = routeMatcher.findTargetForRequestedRoute(httpMethod,
                        uri, acceptType);

                Object target = null;
                if(match != null) {
                    target = match.getTarget();
                }
                else if(httpMethod == HttpMethod.head && bodyContent == null) {
                    // See if get is mapped to provide default head mapping
                    bodyContent = routeMatcher.findTargetForRequestedRoute(
                            HttpMethod.get, uri, acceptType) != null ? ""
                                    : null;
                }
                else if(httpMethod == HttpMethod.options
                        && bodyContent == null) {
                    // CON-476: For an OPTIONS request, attempt to get all the
                    // targets for the route and specify those in the response
                    Set<HttpMethod> methods = routeMatcher
                            .findMethodsForRequestedPath(uri, acceptType);
                    if(!methods.isEmpty()) {
                        httpResponse.setHeader("Allow",
                                StringUtils.join(methods, ','));
                        bodyContent = "";
                    }

                }

                if(target != null) {
                    try {
                        String result = null;
                        if(target instanceof Route) {
                            Route route = ((Route) target);
                            Request request = RequestResponseFactory
                                    .create(match, httpRequest);
                            Response response = RequestResponseFactory
                                    .create(httpResponse);

                            req.setDelegate(request);
                            res.setDelegate(response);

                            Object element = route.handle(req, res);
                            result = route.render(element);
                        }
                        if(result != null) {
                            bodyContent = result;
                        }
                    }
                    catch (HaltException hEx) { // NOSONAR
                        throw hEx; // NOSONAR
                    }
                    catch (Exception e) {
                        Logger.error("", e);
                        httpResponse.setStatus(
                                HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                        bodyContent = INTERNAL_ERROR;
                    }
                }

                // AFTER filters
                matchSet = routeMatcher.findTargetsForRequestedRoute(
                        HttpMethod.after, uri, acceptType);

                for (RouteMatch filterMatch : matchSet) {
                    Object filterTarget = filterMatch.getTarget();
                    if(filterTarget instanceof spark.Filter) {
                        Request request = RequestResponseFactory
                                .create(filterMatch, httpRequest);
                        Response response = RequestResponseFactory
                                .create(httpResponse);

                        req.setDelegate(request);
                        res.setDelegate(response);

                        spark.Filter filter = (spark.Filter) filterTarget;
                        filter.handle(req, res);

                        String bodyAfterFilter = Access.getBody(response);
                        if(bodyAfterFilter != null) {
                            bodyContent = bodyAfterFilter;
                        }
                    }
                }
                // AFTER filters, END
            }
            catch (HaltException hEx) {
                httpResponse.setStatus(hEx.getStatusCode());
                if(hEx.getBody() != null) {
                    bodyContent = hEx.getBody();
                }
                else {
                    bodyContent = "";
                }
            }
        }

        boolean consumed = bodyContent != null;

        if(!consumed && hasOtherHandlers) {
            throw new NotConsumedException();
        }

        if(!consumed && !isServletContext) {
            httpResponse.setStatus(HttpServletResponse.SC_NOT_FOUND);
            bodyContent = String.format(NOT_FOUND, uri);
            consumed = true;
        }

        if(consumed) {
            // Write body content
            if(!httpResponse.isCommitted()) {
                if(httpResponse.getContentType() == null) {
                    httpResponse.setContentType("text/html; charset=utf-8");
                }
                httpResponse.getOutputStream()
                        .write(bodyContent.getBytes("utf-8"));
            }
        }
        else if(chain != null) {
            chain.doFilter(httpRequest, httpResponse);
        }

    }

    public void init(FilterConfig filterConfig) {
        //
    }
}

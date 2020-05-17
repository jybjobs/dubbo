/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.registry.zookeeper;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.URLBuilder;
import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.ConcurrentHashSet;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.common.utils.UrlUtils;
import org.apache.dubbo.registry.NotifyListener;
import org.apache.dubbo.registry.support.FailbackRegistry;
import org.apache.dubbo.remoting.Constants;
import org.apache.dubbo.remoting.zookeeper.ChildListener;
import org.apache.dubbo.remoting.zookeeper.StateListener;
import org.apache.dubbo.remoting.zookeeper.ZookeeperClient;
import org.apache.dubbo.remoting.zookeeper.ZookeeperTransporter;
import org.apache.dubbo.rpc.RpcException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.apache.dubbo.common.constants.CommonConstants.*;
import static org.apache.dubbo.common.constants.RegistryConstants.CATEGORY_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.CONFIGURATORS_CATEGORY;
import static org.apache.dubbo.common.constants.RegistryConstants.CONSUMERS_CATEGORY;
import static org.apache.dubbo.common.constants.RegistryConstants.DEFAULT_CATEGORY;
import static org.apache.dubbo.common.constants.RegistryConstants.DYNAMIC_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.EMPTY_PROTOCOL;
import static org.apache.dubbo.common.constants.RegistryConstants.PROVIDERS_CATEGORY;
import static org.apache.dubbo.common.constants.RegistryConstants.ROUTERS_CATEGORY;

/**
 * ZookeeperRegistry  extend
 *
 */
public class TZookeeperRegistry extends FailbackRegistry {

    private final static Logger logger = LoggerFactory.getLogger(TZookeeperRegistry.class);

    private final static String DEFAULT_ROOT = "dubbo";

    private final static String PROVIDERS = "providers";
    private final static String HOSTS_CATEGORY = "hosts";
    private final static String DEFAULT_HOST_PREFIX = "host";
    private final static String PROTOCOL_TRMI="trmi";
    private final static String OLD_ROOT_PATH="/rkhd/servers";
    private final static String REGISTRY_NAME="registryName"; //consumer调用的 服务名称
    private final static String SERVICE_NAME="serviceName"; // consumer调用的 接口名称

    private final String root;

    private final Set<String> anyServices = new ConcurrentHashSet<>();

    private final ConcurrentMap<URL, ConcurrentMap<NotifyListener, ChildListener>> zkListeners = new ConcurrentHashMap<>();

    private final ZookeeperClient zkClient;

    public TZookeeperRegistry(URL url, ZookeeperTransporter zookeeperTransporter) {
        super(url);
        if (url.isAnyHost()) {
            throw new IllegalStateException("registry address == null");
        }
        String group = url.getParameter(GROUP_KEY, DEFAULT_ROOT);
        if (!group.startsWith(PATH_SEPARATOR)) {
            group = PATH_SEPARATOR + group;
        }
        this.root = group;
        zkClient = zookeeperTransporter.connect(url);
        zkClient.addStateListener((state) -> {
            if (state == StateListener.RECONNECTED) {
                logger.warn("Trying to fetch the latest urls, in case there're provider changes during connection loss.\n" +
                        " Since ephemeral ZNode will not get deleted for a connection lose, " +
                        "there's no need to re-register url of this instance.");
                TZookeeperRegistry.this.fetchLatestAddresses();
            } else if (state == StateListener.NEW_SESSION_CREATED) {
                logger.warn("Trying to re-register urls and re-subscribe listeners of this instance to registry...");
                try {
                    TZookeeperRegistry.this.recover();
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            } else if (state == StateListener.SESSION_LOST) {
                logger.warn("Url of this instance will be deleted from registry soon. " +
                        "Dubbo client will try to re-register once a new session is created.");
            } else if (state == StateListener.SUSPENDED) {

            } else if (state == StateListener.CONNECTED) {

            }
        });
    }

    @Override
    public boolean isAvailable() {
        return zkClient.isConnected();
    }

    @Override
    public void destroy() {
        super.destroy();
        try {
            zkClient.close();
        } catch (Exception e) {
            logger.warn("Failed to close zookeeper client " + getUrl() + ", cause: " + e.getMessage(), e);
        }
    }

    /**
     * 注册服务
     * @param url
     */
    @Override
    public void doRegister(URL url) {
        try {

            if(isTRmi(url.getProtocol())) {// 1. trmi doRegistor
                doTrmiRegister(url);
            }
            if(!isConsumer(url)) { // trmi 的 consumer不进行注册
                zkClient.create(toUrlPath(url), url.getParameter(DYNAMIC_KEY, true));
            }
        } catch (Throwable e) {
            throw new RpcException("Failed to register " + url + " to zookeeper " + getUrl() + ", cause: " + e.getMessage(), e);
        }
    }

    /**
     * 取消服务注册
     * @param url
     */
    @Override
    public void doUnregister(URL url) {
        try {
            if(isTRmi(url.getProtocol())) { //只要有一个接口下线,就下线(是否有问题?)
                doTRmiUnregister(url);
            }
            zkClient.delete(toUrlPath(url));
        } catch (Throwable e) {
            throw new RpcException("Failed to unregister " + url + " to zookeeper " + getUrl() + ", cause: " + e.getMessage(), e);
        }
    }

    /**
     * 订阅服务
     * @param url
     * @param listener
     */
    @Override
    public void doSubscribe(final URL url, final NotifyListener listener) {
        try {
            if (ANY_VALUE.equals(url.getServiceInterface())) {//逻辑保留 todo 支持*
                String root = toRootPath();
                ConcurrentMap<NotifyListener, ChildListener> listeners = zkListeners.computeIfAbsent(url, k -> new ConcurrentHashMap<>());
                ChildListener zkListener = listeners.computeIfAbsent(listener, k -> (parentPath, currentChilds) -> {
                    for (String child : currentChilds) {
                        child = URL.decode(child);
                        if (!anyServices.contains(child)) {
                            anyServices.add(child);
                            subscribe(url.setPath(child).addParameters(INTERFACE_KEY, child,
                                    Constants.CHECK_KEY, String.valueOf(false)), k);
                        }
                    }
                });
                zkClient.create(root, false);
                List<String> services = zkClient.addChildListener(root, zkListener);
                if (CollectionUtils.isNotEmpty(services)) {
                    for (String service : services) {
                        service = URL.decode(service);
                        anyServices.add(service);
                        subscribe(url.setPath(service).addParameters(INTERFACE_KEY, service,
                                Constants.CHECK_KEY, String.valueOf(false)), listener);
                    }
                }
            } else {
                List<URL> urls = new ArrayList<>();
                for (String path : toOldCategoriesPath(url)) {
                    ConcurrentMap<NotifyListener, ChildListener> listeners = zkListeners.computeIfAbsent(url, k -> new ConcurrentHashMap<>());
                    ChildListener zkListener = listeners.computeIfAbsent(listener, k -> (parentPath, currentChilds) -> TZookeeperRegistry.this.notify(url, k, toUrlsWithEmpty(url, parentPath, currentChilds)));
                    zkClient.create(path, false);
                    List<String> children = zkClient.addChildListener(path, zkListener);  // listener /rkhd/servers/{application}/hosts
                    if (children != null) {
                        urls.addAll(toUrlsWithEmpty(url, path, children));
                    }
                }
                notify(url, listener, urls);
            }
        } catch (Throwable e) {
            throw new RpcException("Failed to subscribe " + url + " to zookeeper " + getUrl() + ", cause: " + e.getMessage(), e);
        }
    }

    /**
     * 取消服务订阅
     * @param url
     * @param listener
     */
    @Override
    public void doUnsubscribe(URL url, NotifyListener listener) {
        ConcurrentMap<NotifyListener, ChildListener> listeners = zkListeners.get(url);
        if (listeners != null) {
            ChildListener zkListener = listeners.get(listener);
            if (zkListener != null) {
                if (ANY_VALUE.equals(url.getServiceInterface())) {
                    String root = toRootPath();
                    zkClient.removeChildListener(root, zkListener);
                } else {
                    for (String path : toOldCategoriesPath(url)) { // todo 确认取消订阅时,取消所有这个服务下的所有节点是否有问题
                        zkClient.removeChildListener(path, zkListener);
                    }
                }
            }
        }
    }

    /**
     * 查询注册列表
     * @param url
     * @return
     */
    @Override
    public List<URL> lookup(URL url) {
        if (url == null) {
            throw new IllegalArgumentException("lookup url == null");
        }
        try {
            List<String> providers = new ArrayList<>();
            for (String path : toCategoriesPath(url)) {
                List<String> children = zkClient.getChildren(path);
                if (children != null) {
                    providers.addAll(children);
                }
            }
            return toUrlsWithoutEmpty(url, providers);
        } catch (Throwable e) {
            throw new RpcException("Failed to lookup " + url + " from zookeeper " + getUrl() + ", cause: " + e.getMessage(), e);
        }
    }

    /**
     * 查询next子节点名称,返回null表示不需要创建
     * @param path
     * @return
     */
    public String lookupChildrenPath(String path,String context) {
      final  String   childPath = null;
        try {
            List<String> list =  zkClient.getChildren(path);
            if(list != null && ! list.isEmpty()){
                for (String p:list) {
                    String childrenContext = zkClient.getContent(path+PATH_SEPARATOR+p);
                    if(childrenContext != null && childrenContext.equals(context)){
                        return null;
                    }
                }
                return DEFAULT_HOST_PREFIX+(list.size()+1);
            }
            return DEFAULT_HOST_PREFIX+"1";
        } catch (Throwable e) {
            throw new RpcException("Failed to lookup " + path + " from zookeeper " + getUrl() + ", cause: " + e.getMessage(), e);
        }
    }
    /**
     * 查询所有子节点的值
     * @param path
     * @return
     */
    public  List<String> lookupAllChildrenPath(String path) {
         List<String> childPathList = new ArrayList();
        try {
            List<String> list =  zkClient.getChildren(path);
            if(list != null && ! list.isEmpty()){
                for (String p:list) {
                    String childrenContext = zkClient.getContent(path+PATH_SEPARATOR+p);
                    if(!StringUtils.isEmpty(childrenContext)){
                        childPathList.add(childrenContext);
                    }
                }
            }
            return childPathList;
        } catch (Throwable e) {
            throw new RpcException("Failed to lookup " + path + " from zookeeper " + getUrl() + ", cause: " + e.getMessage(), e);
        }
    }

    /**
     * 查询子节点名称
     * @param path
     * @return
     */
    public String getChildrenPathName(String path,String context) {
        try {
            List<String> list =  zkClient.getChildren(path);
            if(list != null && ! list.isEmpty()){
                for (String p:list) {
                    String childrenContext = zkClient.getContent(p);
                    if(childrenContext != null && childrenContext.equals(context)){
                        return p;
                    }
                }
            }
            return null;
        } catch (Throwable e) {
            throw new RpcException("Failed to lookup " + path + " from zookeeper " + getUrl() + ", cause: " + e.getMessage(), e);
        }
    }

    /**
     * 兼容旧接口调用注册
     * @param url
     */
    public void doTrmiRegister(URL url) {
        try {
            String nodePath = toServiceNodePath(url);
            String context = url.getHost() + ":" + url.getPort();
            String pathName = lookupChildrenPath(nodePath,context);
            if(PROTOCOL_TRMI.equals(url.getProtocol()) && pathName != null) {
                logger.info("Register TRmi serviceNode to zookeeper");
                zkClient.create(nodePath+PATH_SEPARATOR+pathName,context , true);
            }
        } catch (Throwable e) {
            throw new RpcException("Failed to register " + url + " to zookeeper " + getUrl() + ", cause: " + e.getMessage(), e);
        }
    }

    /**
     * 取消旧注册方式注册
     * @param url
     */
    public void doTRmiUnregister(URL url) {
        try {
            String nodePath = toServiceNodePath(url);
            String context = url.getHost() + ":" + url.getPort();
            String pathName = getChildrenPathName(nodePath,context);
            if(pathName != null) {
                zkClient.delete(nodePath+PATH_SEPARATOR+pathName);
            }
        } catch (Throwable e) {
            throw new RpcException("Failed to unregister " + url + " to zookeeper " + getUrl() + ", cause: " + e.getMessage(), e);
        }
    }


    private String toRootDir() {
        if (root.equals(PATH_SEPARATOR)) {
            return root;
        }
        return root + PATH_SEPARATOR;
    }

    private String toRootPath() {
        return root;
    }

    private String toServicePath(URL url) {
        String name = url.getServiceInterface();
        if (ANY_VALUE.equals(name)) {
            return toRootPath();
        }
        return toRootDir() + URL.encode(name);
    }

    private String[] toCategoriesPath(URL url) {
        String[] categories;
        if (ANY_VALUE.equals(url.getParameter(CATEGORY_KEY))) {
            categories = new String[]{PROVIDERS_CATEGORY, CONSUMERS_CATEGORY, ROUTERS_CATEGORY, CONFIGURATORS_CATEGORY};
        } else {
            categories = url.getParameter(CATEGORY_KEY, new String[]{DEFAULT_CATEGORY});
        }
        String[] paths = new String[categories.length];
        for (int i = 0; i < categories.length; i++) {
            paths[i] = toServicePath(url) + PATH_SEPARATOR + categories[i];
        }
        return paths;
    }

    private String[] toOldCategoriesPath(URL url) {
//        String[] categories;
        //if(!isConsumer(url)) return toCategoriesPath(url);//非consumer不处理
//        if (ANY_VALUE.equals(url.getParameter(CATEGORY_KEY))) {
//            categories = new String[]{PROVIDERS_CATEGORY, CONSUMERS_CATEGORY, ROUTERS_CATEGORY, CONFIGURATORS_CATEGORY};
//        } else {
//            categories = url.getParameter(CATEGORY_KEY, new String[]{HOSTS_CATEGORY});
//        }
//        String[] paths = new String[categories.length];
//        for (int i = 0; i < categories.length; i++) {
//            paths[i] = toServiceRootPath(url) + PATH_SEPARATOR + categories[i];
//        }
        return !isConsumer(url) ? toCategoriesPath(url): new String[]{toServiceNodePath(url)};
    }

    private String toCategoryPath(URL url) {
        return toServicePath(url) + PATH_SEPARATOR + url.getParameter(CATEGORY_KEY, DEFAULT_CATEGORY);
    }

    private String toUrlPath(URL url) {
        return toCategoryPath(url) + PATH_SEPARATOR + URL.encode(url.toFullString());
    }

    /**
     * 用于旧注册目录  /rkhd/servers/{application}
     * @param url
     * @return
     */
    private String toServiceRootPath(URL url) {
        String application = (StringUtils.isNotEmpty(url.getParameter(REGISTRY_NAME)) ?
                url.getParameter(REGISTRY_NAME):url.getParameter(CommonConstants.APPLICATION_KEY));
        return  String.format(OLD_ROOT_PATH+PATH_SEPARATOR+application);
    }

    /**
     * 用于旧注册目录  /rkhd/servers/{application}/hosts
     * @param url
     * @return
     */
    private String toServiceNodePath(URL url) {
        return  toServiceRootPath(url)+PATH_SEPARATOR+HOSTS_CATEGORY;
    }

    private List<URL> toOldUrlsWithoutEmpty(URL consumer, List<String> providers,String path) {
        int i = path.lastIndexOf(PATH_SEPARATOR);
        String category = i < 0 ? path : path.substring(i + 1);
        List<URL> urls = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(providers)&& !isConsumer(consumer)) {
            for (String provider : providers) {
                provider = URL.decode(provider);
                if (provider.contains(PROTOCOL_SEPARATOR)) {
                    URL url = URL.valueOf(provider);
                    if (UrlUtils.isMatch(consumer, url)) {
                        urls.add(url);
                    }
                }
            }
        }else if(CommonConstants.CONSUMER.equals(consumer.getProtocol())&& HOSTS_CATEGORY.equals(category)){ // 匹配旧逻辑
            providers = lookupAllChildrenPath(toServiceNodePath(consumer));
            if (CollectionUtils.isNotEmpty(providers)) {
                for (String provider : providers) {
                    String[] ips = provider.split(CommonConstants.GROUP_CHAR_SEPERATOR);
                    if(ips.length<2){
                        logger.warn("Consumer Url provider info error. [provider="+provider+"]");
                        continue;
                    }
                    Map<String,String> params = new HashMap<>();
                           params.putAll(consumer.getParameters());
                    params.put(CATEGORY_KEY,PROVIDERS);
                    String serviceName = StringUtils.isNotEmpty(consumer.getParameter(SERVICE_NAME))?consumer.getParameter(SERVICE_NAME):consumer.getPath();
                    URL url = new URLBuilder(PROTOCOL_TRMI, ips[0], Integer.parseInt(ips[1]),  serviceName, params).build();
                    urls.add(url);
                }
            }
        }
        return urls;
    }

    private List<URL> toUrlsWithoutEmpty(URL consumer, List<String> providers) {
        List<URL> urls = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(providers)) {
            for (String provider : providers) {
                provider = URL.decode(provider);
                if (provider.contains(PROTOCOL_SEPARATOR)) {
                    URL url = URL.valueOf(provider);
                    if (UrlUtils.isMatch(consumer, url)) {
                        urls.add(url);
                    }
                }
            }
        }
        return urls;
    }


//    private List<URL> toOldUrlsWithEmpty(URL consumer, String path, List<String> providers) {
//        List<URL> urls = toUrlsWithoutEmpty(consumer, providers);
//        if (urls == null || urls.isEmpty()) {
//            int i = path.lastIndexOf(PATH_SEPARATOR);
//            String category = i < 0 ? path : path.substring(i + 1);
//            URL empty = URLBuilder.from(consumer)
//                    .setProtocol(EMPTY_PROTOCOL)
//                    .addParameter(CATEGORY_KEY, category)
//                    .build();
//            urls.add(empty);
//        }
//        return urls;
//    }

    private List<URL> toUrlsWithEmpty(URL consumer, String path, List<String> providers) {
        List<URL> urls = toOldUrlsWithoutEmpty(consumer, providers,path);
        if (urls == null || urls.isEmpty()) {
            int i = path.lastIndexOf(PATH_SEPARATOR);
            String category = i < 0 ? path : path.substring(i + 1);
            URL empty = URLBuilder.from(consumer)
                    .setProtocol(EMPTY_PROTOCOL)
                    .addParameter(CATEGORY_KEY, category)
                    .build();
            urls.add(empty);
        }
        return urls;
    }

    /**
     * When zookeeper connection recovered from a connection loss, it need to fetch the latest provider list.
     * re-register watcher is only a side effect and is not mandate.
     */
    private void fetchLatestAddresses() {
        // subscribe
        Map<URL, Set<NotifyListener>> recoverSubscribed = new HashMap<URL, Set<NotifyListener>>(getSubscribed());
        if (!recoverSubscribed.isEmpty()) {
            if (logger.isInfoEnabled()) {
                logger.info("Fetching the latest urls of " + recoverSubscribed.keySet());
            }
            for (Map.Entry<URL, Set<NotifyListener>> entry : recoverSubscribed.entrySet()) {
                URL url = entry.getKey();
                for (NotifyListener listener : entry.getValue()) {
                    addFailedSubscribed(url, listener);
                }
            }
        }
    }

    private boolean isTRmi(String name){
        return PROTOCOL_TRMI.equals(name);
    }

    private boolean isConsumer(URL url){
        return CommonConstants.CONSUMER.equals(url.getProtocol())&& isTRmi(url.getParameter(PROTOCOL_KEY));
    }

}

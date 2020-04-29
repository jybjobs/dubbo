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
package org.apache.dubbo.rpc.protocol.rmi;

import net.sf.cglib.proxy.Enhancer;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.protocol.AbstractProxyProtocol;
import org.apache.dubbo.rpc.proxy.InvokerInvocationHandler;
import org.apache.dubbo.rpc.service.GenericService;
import org.apache.dubbo.rpc.support.ProtocolUtils;

import org.apache.tuscany.sca.interfacedef.Interface;
import org.apache.tuscany.sca.interfacedef.java.JavaInterface;
import net.sf.cglib.asm.ClassWriter;
import net.sf.cglib.asm.Constants;
import net.sf.cglib.asm.Type;
import org.springframework.remoting.RemoteAccessException;
import org.springframework.remoting.rmi.RmiBasedExporter;
import org.springframework.remoting.rmi.RmiProxyFactoryBean;
import org.springframework.remoting.rmi.RmiServiceExporter;
import org.springframework.remoting.support.RemoteInvocation;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.SocketTimeoutException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

import static org.apache.dubbo.common.Version.isRelease263OrHigher;
import static org.apache.dubbo.common.Version.isRelease270OrHigher;
import static org.apache.dubbo.common.constants.CommonConstants.DUBBO_VERSION_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.RELEASE_KEY;
import static org.apache.dubbo.rpc.Constants.GENERIC_KEY;

/**
 * RmiProtocol.
 */
public class TRmiProtocol extends AbstractProxyProtocol {

    public static final int DEFAULT_PORT = 1099;

    public TRmiProtocol() {
        super(RemoteAccessException.class, RemoteException.class);
    }

    private Class clazz;

    @Override
    public int getDefaultPort() {
        return DEFAULT_PORT;
    }

    @Override
    protected <T> Runnable doExport(final T impl, Class<T> type, URL url) throws RpcException {
        RmiServiceExporter rmiServiceExporter = createExporter(impl, type, url, false);
        RmiServiceExporter genericServiceExporter = createExporter(impl, GenericService.class, url, true);
        return new Runnable() {
            @Override
            public void run() {
                try {
                    rmiServiceExporter.destroy();
                    genericServiceExporter.destroy();
                } catch (Throwable e) {
                    logger.warn(e.getMessage(), e);
                }
            }
        };
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <T> T doRefer(final Class<T> serviceType, final URL url) throws RpcException {
        final RmiProxyFactoryBean rmiProxyFactoryBean = new RmiProxyFactoryBean();
        final String generic = url.getParameter(GENERIC_KEY);
        final boolean isGeneric = ProtocolUtils.isGeneric(generic) || serviceType.equals(GenericService.class);
        /*
          RMI needs extra parameter since it uses customized remote invocation object

          The customized RemoteInvocation was firstly introduced in v2.6.3; The package was renamed to 'org.apache.*' since v2.7.0
          Considering the above two conditions, we need to check before sending customized RemoteInvocation:
          1. if the provider version is v2.7.0 or higher, send 'org.apache.dubbo.rpc.protocol.rmi.RmiRemoteInvocation'.
          2. if the provider version is v2.6.3 or higher, send 'com.alibaba.dubbo.rpc.protocol.rmi.RmiRemoteInvocation'.
          3. if the provider version is lower than v2.6.3, does not use customized RemoteInvocation.
         */
        if (isRelease270OrHigher(url.getParameter(RELEASE_KEY))) {
            rmiProxyFactoryBean.setRemoteInvocationFactory(methodInvocation -> {
                RemoteInvocation invocation = new TRmiRemoteInvocation(methodInvocation);
                if (isGeneric) {
                    invocation.addAttribute(GENERIC_KEY, generic);
                }
                return invocation;
            });
        } else if (isRelease263OrHigher(url.getParameter(DUBBO_VERSION_KEY))) {
            rmiProxyFactoryBean.setRemoteInvocationFactory(methodInvocation -> {
                RemoteInvocation invocation = new com.alibaba.dubbo.rpc.protocol.rmi.TRmiRemoteInvocation(methodInvocation);
                if (isGeneric) {
                    invocation.addAttribute(GENERIC_KEY, generic);
                }
                return invocation;
            });
        }
        String serviceUrl = url.toIdentityString();

        /**
         *  exchange protocol
         */
        if(StringUtils.isNotEmpty(serviceUrl)){
            serviceUrl = serviceUrl.replace("trmi://","rmi://");
        }
        if (isGeneric) {
            serviceUrl = serviceUrl + "/" + GENERIC_KEY;
        }
        rmiProxyFactoryBean.setServiceUrl(serviceUrl);
        rmiProxyFactoryBean.setServiceInterface(serviceType);
        rmiProxyFactoryBean.setCacheStub(true);
        rmiProxyFactoryBean.setLookupStubOnStartup(true);
        rmiProxyFactoryBean.setRefreshStubOnConnectFailure(true);
        rmiProxyFactoryBean.afterPropertiesSet();
        return (T) rmiProxyFactoryBean.getObject();
    }

    @Override
    protected int getErrorCode(Throwable e) {
        if (e instanceof RemoteAccessException) {
            e = e.getCause();
        }
        if (e != null && e.getCause() != null) {
            Class<?> cls = e.getCause().getClass();
            if (SocketTimeoutException.class.equals(cls)) {
                return RpcException.TIMEOUT_EXCEPTION;
            } else if (IOException.class.isAssignableFrom(cls)) {
                return RpcException.NETWORK_EXCEPTION;
            } else if (ClassNotFoundException.class.isAssignableFrom(cls)) {
                return RpcException.SERIALIZATION_EXCEPTION;
            }
        }
        return super.getErrorCode(e);
    }

    private <T> RmiServiceExporter createExporter(T impl, Class<?> type, URL url, boolean isGeneric) {
        final RmiServiceExporter rmiServiceExporter = new RmiServiceExporter();
        rmiServiceExporter.setRegistryPort(url.getPort());
        if (isGeneric) {
            rmiServiceExporter.setServiceName(url.getPath() + "/" + GENERIC_KEY);
        } else {
            rmiServiceExporter.setServiceName(url.getPath());
        }


        Class clazz = createInterface(type);
        rmiServiceExporter.setServiceInterface(clazz);
        rmiServiceExporter.setService(createRmiService(impl,rmiServiceExporter,clazz));
        try {
            rmiServiceExporter.afterPropertiesSet();
        } catch (RemoteException e) {
            throw new RpcException(e.getMessage(), e);
        }
        return rmiServiceExporter;
    }

    /**
     * 通过动态代理集成 remote接口,避免RMIServiceExporter进行包装
     */
    protected Class createInterface(Class targetInterfase){
        if (!Remote.class.isAssignableFrom(targetInterfase)) {
            RMIServiceClassLoader classloader = new RMIServiceClassLoader(targetInterfase.getClassLoader());
            final byte[] byteCode = generateRemoteInterface(targetInterfase);
            return classloader.defineClass(targetInterfase.getName(), byteCode);
        }
        return targetInterfase;
    }

    protected Remote createRmiService(Object impl, RmiBasedExporter rmiExporter, Class targetJavaInterface) {
        Enhancer enhancer = new Enhancer();
        //enhancer.setSuperclass(UnicastRemoteObject.class);
       enhancer.setCallback(new MethodProviderInterceptor(impl,rmiExporter));
        enhancer.setInterfaces(new Class[]{targetJavaInterface,Remote.class});
        return (Remote)enhancer.create();
    }
    protected class RMIServiceClassLoader extends ClassLoader {
        public RMIServiceClassLoader(ClassLoader parent) {
            super(parent);
        }

        public Class<?> defineClass(String name, byte[] byteArray) {
            return super.defineClass(name, byteArray, 0, byteArray.length);
        }
    }

    /**
     * if the interface of the component whose serviceBindings must be exposed as RMI Service, does not
     * implement java.rmi.Remote, then generate such an interface. This method will stop with just
     * generating the bytecode. Defining the class from the byte code must be the responsibility of the
     * caller of this method, since it requires a ClassLoader to be created to define and load this interface.
     */
    protected byte[] generateRemoteInterface(Class serviceInterface) {
        String interfazeName = serviceInterface.getName();
        ClassWriter cw = new ClassWriter(false);

        String simpleName = serviceInterface.getSimpleName();
        cw.visit(Constants.V1_5, Constants.ACC_PUBLIC + Constants.ACC_ABSTRACT + Constants.ACC_INTERFACE, interfazeName
                .replace('.', '/'), "java/lang/Object", new String[]{"java/rmi/Remote"}, simpleName + ".java");

        StringBuffer argsAndReturn = null;
        Method[] methods = serviceInterface.getMethods();
        for (Method method : methods) {
            argsAndReturn = new StringBuffer("(");
            Class[] paramTypes = method.getParameterTypes();
            Class returnType = method.getReturnType();

            for (Class paramType : paramTypes) {
                argsAndReturn.append(Type.getType(paramType));
            }
            argsAndReturn.append(")");
            argsAndReturn.append(Type.getType(returnType));

            cw.visitMethod(Constants.ACC_PUBLIC + Constants.ACC_ABSTRACT,
                    method.getName(),
                    argsAndReturn.toString(),
                    new String[]{"java/rmi/RemoteException"},
                    null);
        }
        cw.visitEnd();
        return cw.toByteArray();
    }


}

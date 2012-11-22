/*
 * Copyright (c) Members of the EGEE Collaboration. 2004. 
 * See http://www.eu-egee.org/partners/ for details on the copyright
 * holders.  
 *
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

/*
 * 
 * Authors: L. Zangrando, <zangrando@pd.infn.it>
 *
 */

package org.glite.ce.cream.client.es;

import java.io.IOException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.FactoryConfigurationError;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.apache.axiom.om.OMAbstractFactory;
import org.apache.axiom.om.OMAttribute;
import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.impl.OMNamespaceImpl;
import org.apache.axiom.om.impl.llom.OMElementImpl;
import org.apache.axis2.AxisFault;
import org.apache.axis2.databinding.types.URI;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.GetActivityInfoResponse;
import org.glite.ce.creamapi.ws.es.creation.types.ActivityStatusAttribute;
import org.glite.ce.creamapi.ws.es.creation.types.ActivityStatus_type0;
import org.glite.ce.creamapi.ws.es.creation.types.DirectoryReference;
import org.glite.ce.creamapi.ws.es.glue.ComputingService;
import org.glite.ce.creamapi.ws.es.glue.Service;
import org.glite.ce.creamapi.ws.es.resourceinfo.NotSupportedQueryDialectFault;
import org.glite.ce.creamapi.ws.es.resourceinfo.NotValidQueryStatementFault;
import org.glite.ce.creamapi.ws.es.resourceinfo.types.DefaultQueryDialectsEnumType;
import org.glite.ce.creamapi.ws.es.resourceinfo.types.ExtendedQueryDialectEnumType;
import org.glite.ce.creamapi.ws.es.resourceinfo.types.GetResourceInfo;
import org.glite.ce.creamapi.ws.es.resourceinfo.types.GetResourceInfoResponse;
import org.glite.ce.creamapi.ws.es.resourceinfo.types.QueryExpressionType;
import org.glite.ce.creamapi.ws.es.resourceinfo.types.QueryResourceInfo;
import org.glite.ce.creamapi.ws.es.resourceinfo.types.QueryResourceInfoItem_type0;
import org.glite.ce.creamapi.ws.es.resourceinfo.types.QueryResourceInfoResponse;

public class ResourceInfoClient extends ActivityCommand {

    public static void main(String[] args) {
        List<String> options = new ArrayList<String>(8);
        options.add(EPR);
        options.add(PROXY);
        options.add(GET_RESOURCE_INFO);
        options.add(QUERY_RESOURCE_INFO);

        new ResourceInfoClient(args, options);
    }

    public ResourceInfoClient(String[] args, List<String> options) throws RuntimeException {
        super(args, options);
    }

    private String checkValue(ActivityStatus_type0 activityStatus) {
        if (activityStatus == null || activityStatus.getStatus() == null) {
            return "N/A";
        }

        return activityStatus.getStatus().getValue();
    }

    private String checkValue(ActivityStatusAttribute[] attribute) {
        StringBuffer buffer = new StringBuffer("[ ");

        if (attribute == null) {
            buffer.append("]");
        } else {
            for (int i = 0; i < attribute.length; i++) {
                buffer.append(attribute[i].getValue()).append(", ");
            }

            buffer.replace(buffer.length() - 2, buffer.length(), " ]");
        }
        return buffer.toString();
    }

    private String checkValue(DirectoryReference dir) {
        StringBuffer buffer = new StringBuffer("[ ");

        if (dir == null || dir.getURL() == null) {
            buffer.append("]");
        } else {
            URI[] urlArray = dir.getURL();
            for (int i = 0; i < urlArray.length; i++) {
                buffer.append(urlArray[i].toString()).append(", ");
            }

            buffer.replace(buffer.length() - 2, buffer.length(), " ]");
        }

        return buffer.toString();
    }

    private String checkValue(String value) {
        if (value == null) {
            value = "N/A";
        }

        return value;
    }

    private String checkValue(URI uri) {
        if (uri == null) {
            return "N/A";
        }

        return uri.toString();
    }

    public void execute() {
        if (isGetResourceInfo()) {
            GetResourceInfoResponse response = null;

            try {
                response = getResourceInfoServiceStub().getResourceInfo(new GetResourceInfo());
            } catch (Throwable e) {
                e.printStackTrace();
            }
            // } catch (AxisFault e) {
            // System.out.println(e.getMessage());
            // } catch (RemoteException e) {
            // System.out.println(e.getMessage());
            // } catch (ResourceInfoNotFoundFault e) {
            // System.out.println(e.getFaultMessage().getMessage());
            // } catch (InternalResourceInfoFault e) {
            // System.out.println(e.getFaultMessage().getMessage());
            // } catch
            // (org.glite.ce.creamapi.ws.es.resourceinfo.InternalBaseFault e) {
            // System.out.println(e.getFaultMessage().getInternalBaseFault().getMessage());
            // } catch
            // (org.glite.ce.creamapi.ws.es.resourceinfo.AccessControlFault e) {
            // System.out.println(e.getFaultMessage().getMessage());
            // }

            if (response == null) {
                System.out.println("ResourceInfo not available!");
                return;
            }

            try {
                XMLStreamWriter writer = XMLOutputFactory.newInstance().createXMLStreamWriter(System.out);
                response.serialize(GetResourceInfoResponse.MY_QNAME, writer);
                writer.flush();
                writer.close();
            } catch (Throwable e) {
                e.printStackTrace();
            }
            
        } else if (isQueryResourceInfo()) {
            ExtendedQueryDialectEnumType dialects = new ExtendedQueryDialectEnumType();
            dialects.setObject(DefaultQueryDialectsEnumType._value1);

            System.out.println("query = " + getQuery());
            QueryExpressionType query = new QueryExpressionType();
            OMElementImpl elem = new OMElementImpl("query", new OMNamespaceImpl("http://www.eu-emi.eu", "s"), OMAbstractFactory.getOMFactory());
            elem.setText(getQuery());
            
            query.setExtraElement(elem);

            QueryResourceInfo req = new QueryResourceInfo();
            req.setQueryExpression(query);
            req.setQueryDialect(dialects);

            QueryResourceInfoResponse result = null;

            try {
                result = getResourceInfoServiceStub().queryResourceInfo(req);
            } catch (AxisFault e) {
                System.out.println(e.getMessage());
            } catch (RemoteException e) {
                System.out.println(e.getMessage());
            } catch (org.glite.ce.creamapi.ws.es.resourceinfo.InternalBaseFault e) {
                System.out.println(e.getFaultMessage().getInternalBaseFault().getMessage());
            } catch (org.glite.ce.creamapi.ws.es.resourceinfo.AccessControlFault e) {
                System.out.println(e.getFaultMessage().getMessage());
            } catch (NotValidQueryStatementFault e) {
                System.out.println(e.getFaultMessage().getMessage());
            } catch (NotSupportedQueryDialectFault e) {
                System.out.println(e.getFaultMessage().getMessage());
            }

            if (result == null) {
                System.out.println("query result not available!");
                return;
            }
            
            try {
                // dump the output to console with caching
                XMLStreamWriter writer = XMLOutputFactory.newInstance().createXMLStreamWriter(System.out);
                result.serialize(QueryResourceInfoResponse.MY_QNAME, writer);
                writer.flush();
                writer.close();

            } catch (Throwable e) {
                e.printStackTrace();
            }

            if (result.isQueryResourceInfoItemSpecified()) {
                for (QueryResourceInfoItem_type0 item : result.getQueryResourceInfoItem()) {
                    if (item.getExtraElement() != null) {
                        try {
                            XMLStreamWriter writer = XMLOutputFactory.newInstance().createXMLStreamWriter(System.out);
                            item.getExtraElement().serialize(writer, true);
                            writer.flush();
                            writer.close();
                            System.out.println();
                        } catch (Throwable e) {
                            e.printStackTrace();
                        }
                    } else if (item.getExtraAttributes() != null) {
                        OMAttribute[] attributes = item.getExtraAttributes();

                        for (int i = 0; i < attributes.length; i++) {
                            System.out.println(attributes[i].getLocalName() + "=" + attributes[i].getAttributeValue());
                        }
                    }
                }
            } else {
                System.out.println("none result found!");
            }
        }
    }
}

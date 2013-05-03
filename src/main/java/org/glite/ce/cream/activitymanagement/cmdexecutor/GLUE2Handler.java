package org.glite.ce.cream.activitymanagement.cmdexecutor;

import java.io.ByteArrayInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Properties;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamWriter;

import org.apache.axiom.om.OMAbstractFactory;
import org.apache.axiom.om.OMAttribute;
import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.impl.builder.StAXBuilder;
import org.apache.axiom.om.impl.builder.StAXOMBuilder;
import org.apache.axiom.om.xpath.AXIOMXPath;
import org.apache.axis2.databinding.types.URI;
import org.apache.axis2.databinding.types.UnsignedInt;
import org.apache.axis2.databinding.types.UnsignedLong;
import org.apache.log4j.Logger;
import org.glite.ce.creamapi.ws.es.glue.AccessPolicy_t;
import org.glite.ce.creamapi.ws.es.glue.ApplicationEnvironment_t;
import org.glite.ce.creamapi.ws.es.glue.ApplicationEnvironments_type0;
import org.glite.ce.creamapi.ws.es.glue.Associations_type12;
import org.glite.ce.creamapi.ws.es.glue.BenchmarkType_t;
import org.glite.ce.creamapi.ws.es.glue.Benchmark_t;
import org.glite.ce.creamapi.ws.es.glue.Capability_t;
import org.glite.ce.creamapi.ws.es.glue.ComputingEndpoint_t;
import org.glite.ce.creamapi.ws.es.glue.ComputingManagerType_t;
import org.glite.ce.creamapi.ws.es.glue.ComputingManager_t;
import org.glite.ce.creamapi.ws.es.glue.ComputingService;
import org.glite.ce.creamapi.ws.es.glue.ComputingService_t;
import org.glite.ce.creamapi.ws.es.glue.ComputingShare_t;
import org.glite.ce.creamapi.ws.es.glue.DN_t;
import org.glite.ce.creamapi.ws.es.glue.EndpointHealthState_t;
import org.glite.ce.creamapi.ws.es.glue.EndpointTechnology_t;
import org.glite.ce.creamapi.ws.es.glue.ExtendedBoolean_t;
import org.glite.ce.creamapi.ws.es.glue.InterfaceName_t;
import org.glite.ce.creamapi.ws.es.glue.JobDescription_t;
import org.glite.ce.creamapi.ws.es.glue.MappingPolicy_t;
import org.glite.ce.creamapi.ws.es.glue.PolicyScheme_t;
import org.glite.ce.creamapi.ws.es.glue.QualityLevel_t;
import org.glite.ce.creamapi.ws.es.glue.ReservationPolicy_t;
import org.glite.ce.creamapi.ws.es.glue.SchedulingPolicy_t;
import org.glite.ce.creamapi.ws.es.glue.ServiceType_t;
import org.glite.ce.creamapi.ws.es.glue.ServingState_t;
import org.glite.ce.creamapi.ws.es.glue.Staging_t;
import org.glite.ce.creamapi.ws.es.glue.ToStorageService_t;

public class GLUE2Handler extends Thread {
    private static Logger logger = Logger.getLogger(GLUE2Handler.class);
    private int rate = -1; //minutes
    private boolean terminate = false;
    private OMElement computingServiceElement = null;
    private ComputingService_t computingService = null;
    private DirContext ctx = null;

    public static void main(String[] args) {
        GLUE2Handler glue2Handler = null;
        try {
            glue2Handler = new GLUE2Handler("ldap://cream-36.pd.infn.it:2170", 60);
        } catch (Exception e1) {
            e1.printStackTrace();
            return;
        }

        try {
            System.in.read();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        try {
            ComputingService_t computingS = glue2Handler.getComputingService();

            //dump the output to console with caching
            XMLStreamWriter writer = XMLOutputFactory.newInstance().createXMLStreamWriter(new
            FileWriter("/tmp/pippo3.xml"));
            computingS.serialize(ComputingService.MY_QNAME, writer);
            writer.flush();
            writer.close();
        
            System.out.println(ComputingService.MY_QNAME.getLocalPart());
            System.out.println(ComputingService.MY_QNAME.getPrefix());
        } catch (Throwable e) {
            e.printStackTrace();
        }

        try {
            // XPath xPath = XPathFactory.newInstance().newXPath();
            // xPath.setNamespaceContext(new NamespaceContext() {
            // public String getNamespaceURI( String prefix) {
            // if (prefix.equals("ns")) {
            // return "http://schemas.ogf.org/glue/2009/03/spec_2.0_r1";
            // }
            // return "http://schemas.ogf.org/glue/2009/03/spec_2.0_r1";
            // }
            //
            // public String getPrefix( String namespaceURI) {
            // if (
            // namespaceURI.equals("http://schemas.ogf.org/glue/2009/03/spec_2.0_r1"))
            // {
            // return "ns";
            // }
            //
            // return null;
            // }
            //
            // public Iterator getPrefixes( String namespaceURI) {
            // ArrayList list = new ArrayList();
            //
            // if (
            // namespaceURI.equals("http://schemas.ogf.org/glue/2009/03/spec_2.0_r1"))
            // {
            // list.add("ns");
            // }
            // return list.iterator();
            // }
            // });
            //
            // System.out.println(xPath.getNamespaceContext().toString());
            // //XPathExpression xPathExpression =
            // xPath.compile("/ComputingService/ComputingShare");
            // InputSource inputSource = new InputSource(new
            // FileInputStream("/tmp/pippo3.xml"));
            // DTMNodeList nodes = (DTMNodeList)
            // xPath.evaluate("/ns:ComputingService/ns:ComputingShare",
            // inputSource, XPathConstants.NODESET);
            // System.out.println(nodes.getLength());

            // XMLStreamReader xmlStreamReader =
            // XMLInputFactory.newInstance().createXMLStreamReader(new
            // FileInputStream("/home/zangrand/glue.txt"));
            // StAXOMBuilder stAXOMBuilder = new StAXOMBuilder(xmlStreamReader);
            //
            // OMElement documentElement = stAXOMBuilder.getDocumentElement();
            // //documentElement.declareDefaultNamespace(null);
            // System.out.println(documentElement.getDefaultNamespace().toString());
            // System.out.println("\"" +
            // documentElement.getDefaultNamespace().getNamespaceURI() + "\"");
            // System.out.println("\"" +
            // documentElement.getDefaultNamespace().getPrefix() + "\"");
            //
            // AXIOMXPath xpathExpression = new
            // AXIOMXPath("/ComputingService/ComputingShare");
            // AXIOMXPath xpathExpression = new
            // AXIOMXPath("/xmlns:ComputingService/xmlns:ComputingShare");
            // xpathExpression.addNamespace("xmlns",
            // "http://schemas.ogf.org/glue/2009/03/spec_2.0_r1");
            //
            // //OMElement nameOM = (OMElement)
            // xpathExpression.selectSingleNode(documentElement);
            //
            //
            // List<OMElement> selectedNode = (List<OMElement>)
            // xpathExpression.selectNodes(documentElement);
            // for(OMElement node : selectedNode) {
            // System.out.println(node.getLocalName());
            // }
            //
            // xmlStreamReader.close();

            // String xmlStream =
            // computingS.getOMElement(ComputingService.MY_QNAME,
            // OMAbstractFactory.getOMFactory()).toString();
            // //.toStringWithConsume();
            //
            // StAXBuilder builder = new StAXOMBuilder(new
            // ByteArrayInputStream(xmlStream.getBytes()));
            // OMElement root = builder.getDocumentElement();

            // AXIOMXPath xpathExpression = new
            // AXIOMXPath("/ns:ComputingService/ns:ComputingShare");
            // xpathExpression.addNamespace("ns",
            // computingServiceElement.getNamespaceURI());
            // List<OMElement> selectedNode = (List<OMElement>)
            // xpathExpression.selectNodes(computingServiceElement);

          //  List selectedNode = glue2Handler.executeXPathQuery("/ComputingService/ComputingEndpoint/ID");ComputingShare
            List selectedNode = glue2Handler.executeXPathQuery("/ComputingService/ComputingShare");
            // List selectedNode =
            // glue2Handler.executeXPathQuery("/ComputingService/ComputingEndpoint");
            System.out.println(selectedNode.size());

            for (int i = 0; i < selectedNode.size(); i++) {
                if (selectedNode.get(i) instanceof OMAttribute) {
                    System.out.println(((OMAttribute) selectedNode.get(i)).getAttributeValue());
                } else if (selectedNode.get(i) instanceof OMElement) {
                    ((OMElement) selectedNode.get(i)).serialize(System.out);
                    System.out.println();
                }
            }

            // selectedNode =
            // glue2Handler.executeXPathQuery("/ns:ComputingService/ns:ComputingEndpoint");
            // System.out.println(selectedNode.size());
            //
            // for(OMElement node : selectedNode) {
            // System.out.println(node.getLocalName());
            // }

            // xpathExpression = new
            // AXIOMXPath("/ns:ComputingService/ns:ComputingShare");
            // xpathExpression.addNamespace("ns",
            // computingServiceElement.getNamespaceURI());
            // selectedNode = (List<OMElement>)
            // xpathExpression.selectNodes(computingServiceElement);
            //
            // System.out.println(selectedNode.size());
            //
            // for(OMElement node : selectedNode) {
            // System.out.println(node.getLocalName());
            // }

            // OMElement root =
            // computingS.getOMElement(ComputingService.MY_QNAME,
            // OMAbstractFactory.getOMFactory());
            // OMNamespace ns = new
            // OMNamespaceImpl("http://schemas.ogf.org/glue/2009/03/spec_2.0_r1",
            // "ns");
            //
            // root.setNamespace(ns );
            // System.out.println(root.getNamespaceURI());
            // System.out.println("\"" +
            // root.getDefaultNamespace().getNamespaceURI() + "\"");
            // System.out.println("\"" + root.getDefaultNamespace().getPrefix()
            // + "\"");
            //
            // //computingS.serialize(ComputingService.MY_QNAME,
            // XMLOutputFactory.newInstance().createXMLStreamWriter(new
            // FileWriter("/tmp/pippo2.xml")));
            // AXIOMXPath xpathExpression = new
            // AXIOMXPath("/ns:ComputingService/ns:ComputingEndpoint");
            // xpathExpression.addNamespace("ns", root.getNamespaceURI());
            // List<OMElement> nodeList =
            // (List<OMElement>)xpathExpression.selectNodes(root);
            //
            // System.out.println(xpathExpression.getNamespaces().size());
            // System.out.println(nodeList.size());
            // for(OMElement node : nodeList) {
            // System.out.println(node.getLocalName());
            // }
            // System.out.println(computingS.getOMElement(ComputingService.MY_QNAME,
            // OMAbstractFactory.getOMFactory()).toStringWithConsume());
        } catch (Throwable e) {
            e.printStackTrace();
        }

        glue2Handler.terminate();
    }

    public GLUE2Handler(String url, int rate) throws Exception {
        setDaemon(true);
        if (rate <= 0) {
            this.rate = 3600000; //1 hour
        } else {
            this.rate = rate * 60000;
        }

        logger.info("[url=" + url + " rate=" + rate + "]");
        
        Properties env = new Properties();
        env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        env.put(Context.PROVIDER_URL, url);

        ctx = new InitialDirContext(env);

        start();
    }

    public List executeXPathQuery(String query) throws Exception {
        logger.debug("BEGIN executeXPathQuery");
        if (query == null || query.length() == 0) {
            throw new IllegalArgumentException("xpath query not defined!");
        }

        if (computingServiceElement == null) {
            throw new Exception("GLUE2 not yet available!");
        }

        AXIOMXPath xpathExpression = new AXIOMXPath(query);
        xpathExpression.addNamespace("xsi", "http://www.w3.org/2001/XMLSchema-instance");
        
        List selectedNode = xpathExpression.selectNodes(computingServiceElement);

        logger.debug("END executeXPathQuery: found " + selectedNode.size() + " items");
        return selectedNode;
    }

    private BenchmarkType_t getAttributeValueAsBenchmarkType(Attribute attribute) {
        BenchmarkType_t value = null;

        if (attribute != null && attribute.size() > 0) {
            try {
                value = new BenchmarkType_t();
                value.setBenchmarkType_t((String) attribute.get(0));
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }

        return value;
    }

    private BenchmarkType_t[] getAttributeValueAsBenchmarkTypeArray(Attribute attribute) {
        BenchmarkType_t[] value = null;

        if (attribute != null && attribute.size() > 0) {
            value = new BenchmarkType_t[attribute.size()];
            try {
                for (int i = 0; i < attribute.size(); i++) {
                    value[i] = new BenchmarkType_t();
                    value[i].setBenchmarkType_t((String) attribute.get(0));
                }
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }

        return value;
    }

    private Capability_t[] getAttributeValueAsCapability(Attribute attribute) {
        Capability_t[] value = null;

        if (attribute != null && attribute.size() > 0) {
            value = new Capability_t[attribute.size()];

            try {
                for (int i = 0; i < attribute.size(); i++) {
                    value[i] = new Capability_t();
                    value[i].setCapability_t((String) attribute.get(i));
                }
            } catch (NamingException e) {
                e.printStackTrace();
            }
        }

        return value;
    }

    private ComputingManagerType_t getAttributeValueAsComputingManagerType(Attribute attribute) {
        ComputingManagerType_t value = null;

        if (attribute != null && attribute.size() > 0) {
            try {
                value = new ComputingManagerType_t();
                value.setComputingManagerType_t((String) attribute.get(0));
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }

        return value;
    }

    private Calendar getAttributeValueAsDateTime(Attribute attribute) {
        Calendar dateTime = null;

        if (attribute != null && attribute.size() > 0) {
            dateTime = GregorianCalendar.getInstance();
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
            dateFormat.setCalendar(dateTime);
            String value = null;
            try {
                value = (String) attribute.get(0);
                dateFormat.parse(value);
            } catch (NamingException e) {
                logger.error("getAttributeValueAsDateTime: found wrong datetime value (" + value + ")");
            } catch (ParseException e) {
                logger.error("getAttributeValueAsDateTime: cannot parse the datetime value (" + value + ")");
            }
        }

        return dateTime;
    }

    private DN_t getAttributeValueAsDN(Attribute attribute) {
        DN_t value = null;

        if (attribute != null && attribute.size() > 0) {
            try {
                value = new DN_t();
                value.setDN_t((String) attribute.get(0));
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }

        return value;
    }

    private DN_t[] getAttributeValueAsDNArray(Attribute attribute) {
        DN_t value[] = null;

        if (attribute != null && attribute.size() > 0) {
            value = new DN_t[attribute.size()];

            try {
                for (int i = 0; i < attribute.size(); i++) {
                    value[i] = new DN_t();
                    value[i].setDN_t((String) attribute.get(i));
                }
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }

        return value;
    }

    private EndpointHealthState_t getAttributeValueAsEndpointHealthState(Attribute attribute) {
        EndpointHealthState_t value = null;

        if (attribute != null && attribute.size() > 0) {
            try {
                value = EndpointHealthState_t.Factory.fromValue((String) attribute.get(0));
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }

        return value;
    }

    // private Associations_type0 getAttributeValueAsAssociations(Attribute
    // attribute) {
    // Associations_type0 value = null;
    //
    // if (attribute != null && attribute.size() > 0) {
    // value = new Associations_type0();
    // value.setUserDomainID(param)
    //
    // try {
    // dateFormat.parse((String) attribute.get(0));
    // } catch (Throwable t) {
    // t.printStackTrace();
    // }
    //
    // System.out.println("getAttributeValueAsDateTime: " + date.getTime());
    // }
    // return value;
    // }

    private EndpointTechnology_t getAttributeValueAsEndpointTechnology(Attribute attribute) {
        EndpointTechnology_t value = null;

        if (attribute != null && attribute.size() > 0) {
            try {
                value = new EndpointTechnology_t();
                value.setEndpointTechnology_t((String) attribute.get(0));
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }

        return value;
    }

    private ExtendedBoolean_t getAttributeValueAsExtendedBoolean(Attribute attribute) {
        ExtendedBoolean_t value = null;

        if (attribute != null && attribute.size() > 0) {
            try {
                value = ExtendedBoolean_t.Factory.fromValue((String) attribute.get(0));
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }

        return value;
    }

    private Float getAttributeValueAsFloat(Attribute attribute) {
        Float value = null;

        if (attribute != null && attribute.size() > 0) {
            try {
                value = Float.valueOf((String) attribute.get(0));
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }

        return value;
    }

    private InterfaceName_t getAttributeValueAsInterfaceName(Attribute attribute) {
        InterfaceName_t value = null;

        if (attribute != null && attribute.size() > 0) {
            try {
                value = new InterfaceName_t();
                value.setInterfaceName_t((String) attribute.get(0));
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }

        return value;
    }

    private JobDescription_t[] getAttributeValueAsJobDescriptionArray(Attribute attribute) {
        JobDescription_t[] value = null;

        if (attribute != null && attribute.size() > 0) {
            value = new JobDescription_t[attribute.size()];

            try {
                for (int i = 0; i < attribute.size(); i++) {
                    value[i] = new JobDescription_t();
                    value[i].setJobDescription_t((String) attribute.get(i));
                }
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }

        return value;
    }

    private PolicyScheme_t getAttributeValueAsPolicyScheme(Attribute attribute) {
        PolicyScheme_t value = null;

        if (attribute != null && attribute.size() > 0) {
            try {
                value = new PolicyScheme_t();
                value.setPolicyScheme_t((String) attribute.get(0));
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }

        return value;
    }

    private QualityLevel_t getAttributeValueAsQualityLevel(Attribute attribute) {
        QualityLevel_t value = null;

        if (attribute != null && attribute.size() > 0) {
            try {
                value = QualityLevel_t.Factory.fromValue((String) attribute.get(0));
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }

        return value;
    }

    private ReservationPolicy_t getAttributeValueAsReservationPolicy(Attribute attribute) {
        ReservationPolicy_t value = null;

        if (attribute != null && attribute.size() > 0) {
            try {
                value = ReservationPolicy_t.Factory.fromValue((String) attribute.get(0));
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }

        return value;
    }

    private SchedulingPolicy_t getAttributeValueAsSchedulingPolicy(Attribute attribute) {
        SchedulingPolicy_t value = null;

        if (attribute != null && attribute.size() > 0) {
            try {
                value = new SchedulingPolicy_t();
                value.setSchedulingPolicy_t((String) attribute.get(0));
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }

        return value;
    }

    private ServiceType_t getAttributeValueAsServiceType(Attribute attribute) {
        ServiceType_t value = null;

        if (attribute != null && attribute.size() > 0) {
            try {
                value = new ServiceType_t();
                value.setServiceType_t((String) attribute.get(0));
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }

        return value;
    }

    private ServingState_t getAttributeValueAsServingState(Attribute attribute) {
        ServingState_t value = null;

        if (attribute != null && attribute.size() > 0) {
            try {
                value = ServingState_t.Factory.fromValue((String) attribute.get(0));
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }

        return value;
    }

    private Staging_t getAttributeValueAsStaging(Attribute attribute) {
        Staging_t value = null;

        if (attribute != null && attribute.size() > 0) {
            try {
                value = Staging_t.Factory.fromValue((String) attribute.get(0));
            } catch (NamingException e) {
                e.printStackTrace();
            }
        }

        return value;
    }

    private String getAttributeValueAsString(Attribute attribute) {
        String value = null;

        if (attribute != null && attribute.size() > 0) {
            try {
                value = (String) attribute.get(0);
            } catch (NamingException e) {
                e.printStackTrace();
            }
        }

        return value;
    }

    private String[] getAttributeValueAsStringArray(Attribute attribute) {
        String[] value = null;

        if (attribute != null && attribute.size() > 0) {
            value = new String[attribute.size()];

            try {
                for (int i = 0; i < attribute.size(); i++) {
                    value[i] = (String) attribute.get(i);
                }
            } catch (NamingException e) {
                e.printStackTrace();
            }
        }

        return value;
    }

    private UnsignedInt getAttributeValueAsUnsignedInt(Attribute attribute) {
        UnsignedInt value = null;

        if (attribute != null && attribute.size() > 0) {
            try {
                value = new UnsignedInt((String) attribute.get(0));
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }

        return value;
    }

    private UnsignedLong getAttributeValueAsUnsignedLong(Attribute attribute) {
        UnsignedLong value = null;

        if (attribute != null && attribute.size() > 0) {
            try {
                value = new UnsignedLong((String) attribute.get(0));
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }

        return value;
    }

    private URI getAttributeValueAsURI(Attribute attribute) {
        URI value = null;

        if (attribute != null && attribute.size() > 0) {
            try {
                value = new URI((String) attribute.get(0));
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }

        return value;
    }

    private URI[] getAttributeValueAsURIArray(Attribute attribute) {
        URI[] value = null;

        if (attribute != null && attribute.size() > 0) {
            value = new URI[attribute.size()];

            try {
                for (int i = 0; i < attribute.size(); i++) {
                    value[i] = new URI((String) attribute.get(i));
                }
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }

        return value;
    }

    public ComputingService_t getComputingService() throws Exception {
        synchronized (computingService) {
            if (computingService == null) {
                throw new Exception("GLUE2 not yet available!");
            }

            return computingService;
        }
    }

    private AccessPolicy_t[] makeAccessPolicies(DirContext ctx, String computingEndpointId) throws NamingException {
        Attributes attributes = null;
        SearchResult searchResult = null;
        SearchControls ctls = new SearchControls();
        ctls.setSearchScope(SearchControls.SUBTREE_SCOPE);
        NamingEnumeration<SearchResult> answer = ctx.search("o=glue", "(&(GLUE2AccessPolicyEndpointForeignKey=" + computingEndpointId + ")(objectclass=GLUE2AccessPolicy))", ctls);
        AccessPolicy_t accessPolicy = null;
        List<AccessPolicy_t> accessPolicies = new ArrayList<AccessPolicy_t>(0);

        while (answer.hasMoreElements()) {
            searchResult = answer.nextElement();
            // System.out.println(searchResult.getNameInNamespace());

            attributes = searchResult.getAttributes();

            if (attributes != null) {
                // printAttributes(attributes);

                accessPolicy = new AccessPolicy_t();
                // accessPolicy.setAssociations(param);
                accessPolicy.setCreationTime(getAttributeValueAsDateTime(attributes.get("GLUE2EntityCreationTime")));
                // accessPolicy.setExtensions(param);
                accessPolicy.setID(getAttributeValueAsURI(attributes.get("GLUE2PolicyID")));
                accessPolicy.setName(getAttributeValueAsString(attributes.get("GLUE2EntityName")));
                accessPolicy.setOtherInfo(getAttributeValueAsStringArray(attributes.get("GLUE2EntityOtherInfo")));
                accessPolicy.setRule(getAttributeValueAsStringArray(attributes.get("GLUE2PolicyRule")));
                accessPolicy.setScheme(getAttributeValueAsPolicyScheme(attributes.get("GLUE2PolicyScheme")));
                accessPolicy.setValidity(getAttributeValueAsUnsignedLong(attributes.get("GLUE2EntityValidity")));

                accessPolicies.add(accessPolicy);
            }
        }

        AccessPolicy_t[] result = null;

        if (accessPolicies.size() > 0) {
            result = new AccessPolicy_t[accessPolicies.size()];
            result = accessPolicies.toArray(result);
        }

        return result;
    }

    private ApplicationEnvironment_t[] makeApplicationEnvironments(DirContext ctx, String computingManagerId) throws NamingException {
        Attributes attributes = null;
        SearchResult searchResult = null;
        SearchControls ctls = new SearchControls();
        ctls.setSearchScope(SearchControls.SUBTREE_SCOPE);
        NamingEnumeration<SearchResult> answer = ctx.search("o=glue", "(&(GLUE2ApplicationEnvironmentComputingManagerForeignKey=" + computingManagerId
                + ")(objectclass=GLUE2ApplicationEnvironment))", ctls);
        ApplicationEnvironment_t applicationEnvironment = null;
        List<ApplicationEnvironment_t> applicationEnvironments = new ArrayList<ApplicationEnvironment_t>(0);

        while (answer.hasMoreElements()) {
            searchResult = answer.nextElement();
            // System.out.println(searchResult.getNameInNamespace());

            attributes = searchResult.getAttributes();

            if (attributes != null) {
                // printAttributes(attributes);

                applicationEnvironment = new ApplicationEnvironment_t();
                // applicationEnvironment.setApplicationHandle(param);
                applicationEnvironment.setAppName(getAttributeValueAsString(attributes.get("GLUE2ApplicationEnvironmentAppName")));
                applicationEnvironment.setAppVersion(getAttributeValueAsString(attributes.get("GLUE2ApplicationEnvironmentAppVersion")));
                // applicationEnvironment.setAssociations(param);
                applicationEnvironment.setBestBenchmark(getAttributeValueAsBenchmarkTypeArray(attributes.get("GLUE2ApplicationEnvironmentBestBenchmark")));
                applicationEnvironment.setCreationTime(getAttributeValueAsDateTime(attributes.get("GLUE2EntityCreationTime")));
                applicationEnvironment.setDescription(getAttributeValueAsString(attributes.get("GLUE2ApplicationEnvironmentDescription")));
                // applicationEnvironment.setExtensions(param);
                applicationEnvironment.setFreeJobs(getAttributeValueAsUnsignedInt(attributes.get("GLUE2ApplicationEnvironmentFreeJobs")));
                applicationEnvironment.setFreeSlots(getAttributeValueAsUnsignedInt(attributes.get("GLUE2ApplicationEnvironmentFreeSlots")));
                applicationEnvironment.setFreeUserSeats(getAttributeValueAsUnsignedInt(attributes.get("GLUE2ApplicationEnvironmentFreeUserSeats")));
                applicationEnvironment.setID(getAttributeValueAsURI(attributes.get("GLUE2ApplicationEnvironmentID")));
                // applicationEnvironment.setLicense(param);
                applicationEnvironment.setMaxJobs(getAttributeValueAsUnsignedInt(attributes.get("GLUE2ApplicationEnvironmentMaxJobs")));
                applicationEnvironment.setMaxSlots(getAttributeValueAsUnsignedInt(attributes.get("GLUE2ApplicationEnvironmentMaxSlots")));
                applicationEnvironment.setMaxUserSeats(getAttributeValueAsUnsignedInt(attributes.get("GLUE2ApplicationEnvironmentMaxUserSeats")));
                applicationEnvironment.setName(getAttributeValueAsString(attributes.get("GLUE2EntityName")));
                applicationEnvironment.setOtherInfo(getAttributeValueAsStringArray(attributes.get("GLUE2EntityOtherInfo")));
                // applicationEnvironment.setParallelSupport(param);
                applicationEnvironment.setRemovalDate(getAttributeValueAsDateTime(attributes.get("GLUE2ApplicationEnvironmentRemovalDate")));
                // applicationEnvironment.setState(param);
                applicationEnvironment.setValidity(getAttributeValueAsUnsignedLong(attributes.get("GLUE2EntityValidity")));

                applicationEnvironments.add(applicationEnvironment);
            }
        }

        ApplicationEnvironment_t[] result = null;

        if (applicationEnvironments.size() > 0) {
            result = new ApplicationEnvironment_t[applicationEnvironments.size()];
            result = applicationEnvironments.toArray(result);
        }

        return result;
    }

    private Benchmark_t[] makeBenchmarks(DirContext ctx, String computingManagerId) throws NamingException {
        Attributes attributes = null;
        SearchResult searchResult = null;
        SearchControls ctls = new SearchControls();
        ctls.setSearchScope(SearchControls.SUBTREE_SCOPE);
        NamingEnumeration<SearchResult> answer = ctx.search("o=glue", "(&(GLUE2BenchmarkComputingManagerForeignKey=" + computingManagerId + ")(objectclass=GLUE2Benchmark))", ctls);
        Benchmark_t benchmark = null;
        List<Benchmark_t> benchmarks = new ArrayList<Benchmark_t>(0);

        while (answer.hasMoreElements()) {
            searchResult = answer.nextElement();
            // System.out.println(searchResult.getNameInNamespace());

            attributes = searchResult.getAttributes();

            if (attributes != null) {
                // printAttributes(attributes);

                benchmark = new Benchmark_t();
                benchmark.setCreationTime(getAttributeValueAsDateTime(attributes.get("GLUE2EntityCreationTime")));
                // benchmark.setExtensions(param);
                benchmark.setID(getAttributeValueAsURI(attributes.get("GLUE2BenchmarkID")));
                benchmark.setName(getAttributeValueAsString(attributes.get("GLUE2EntityName")));
                benchmark.setOtherInfo(getAttributeValueAsStringArray(attributes.get("GLUE2EntityOtherInfo")));
                benchmark.setType(getAttributeValueAsBenchmarkType(attributes.get("GLUE2BenchmarkType")));
                benchmark.setValidity(getAttributeValueAsUnsignedLong(attributes.get("GLUE2EntityValidity")));
                benchmark.setValue(getAttributeValueAsFloat(attributes.get("GLUE2BenchmarkValue")));

                benchmarks.add(benchmark);
            }
        }

        Benchmark_t[] result = null;

        if (benchmarks.size() > 0) {
            result = new Benchmark_t[benchmarks.size()];
            result = benchmarks.toArray(result);
        }

        return result;
    }

    private ComputingEndpoint_t[] makeComputingEndpoints(DirContext ctx, String computingServiceId) throws NamingException {
        Attributes attributes = null;
        SearchResult searchResult = null;
        SearchControls ctls = new SearchControls();
        ctls.setSearchScope(SearchControls.SUBTREE_SCOPE);
        NamingEnumeration<SearchResult> answer = ctx.search("o=glue", "(&(GLUE2EndpointServiceForeignKey=" + computingServiceId + ")(objectclass=GLUE2ComputingEndpoint))", ctls);
        ComputingEndpoint_t computingEndpoint = null;
        List<ComputingEndpoint_t> computingEndpoints = new ArrayList<ComputingEndpoint_t>(0);

        while (answer.hasMoreElements()) {
            searchResult = answer.nextElement();
            // System.out.println(searchResult.getNameInNamespace());

            attributes = searchResult.getAttributes();

            if (attributes != null) {
                // printAttributes(attributes);

                computingEndpoint = new ComputingEndpoint_t();
                // computingEndpoint.setAssociations(param);
                computingEndpoint.setCapability(getAttributeValueAsCapability(attributes.get("GLUE2EndpointCapability")));
                // computingEndpoint.setComputingActivities(param);
                computingEndpoint.setCreationTime(getAttributeValueAsDateTime(attributes.get("GLUE2EntityCreationTime")));
                computingEndpoint.setDowntimeAnnounce(getAttributeValueAsDateTime(attributes.get("GLUE2EndpointDowntimeAnnounce")));
                computingEndpoint.setDowntimeEnd(getAttributeValueAsDateTime(attributes.get("GLUE2EndpointDowntimeEnd")));
                computingEndpoint.setDowntimeInfo(getAttributeValueAsString(attributes.get("GLUE2EndpointDowntimeInfo")));
                computingEndpoint.setDowntimeStart(getAttributeValueAsDateTime(attributes.get("GLUE2EndpointDowntimeStart")));
                // computingEndpoint.setExtensions(getAttributeValueAsExtensions(attributes.get("GLUE2EntityExtensions")));
                computingEndpoint.setHealthState(getAttributeValueAsEndpointHealthState(attributes.get("GLUE2EndpointHealthState")));
                computingEndpoint.setHealthStateInfo(getAttributeValueAsString(attributes.get("GLUE2EndpointHealthStateInfo")));
                computingEndpoint.setID(getAttributeValueAsURI(attributes.get("GLUE2EndpointID")));
                computingEndpoint.setImplementationName(getAttributeValueAsString(attributes.get("GLUE2EndpointImplementationName")));
                computingEndpoint.setImplementationVersion(getAttributeValueAsString(attributes.get("GLUE2EndpointImplementationVersion")));
                computingEndpoint.setImplementor(getAttributeValueAsString(attributes.get("GLUE2EndpointImplementor")));
                computingEndpoint.setInterfaceExtension(getAttributeValueAsURIArray(attributes.get("GLUE2EndpointInterfaceExtension")));
                computingEndpoint.setInterfaceName(getAttributeValueAsInterfaceName(attributes.get("GLUE2EndpointInterfaceName")));
                computingEndpoint.setInterfaceVersion(getAttributeValueAsStringArray(attributes.get("GLUE2EndpointInterfaceVersion")));
                computingEndpoint.setIssuerCA(getAttributeValueAsDN(attributes.get("GLUE2EndpointIssuerCA")));
                computingEndpoint.setJobDescription(getAttributeValueAsJobDescriptionArray(attributes.get("GLUE2ComputingEndpointJobDescription")));
                computingEndpoint.setName(getAttributeValueAsString(attributes.get("GLUE2EntityName")));
                computingEndpoint.setOtherInfo(getAttributeValueAsStringArray(attributes.get("GLUE2EntityOtherInfo")));
                computingEndpoint.setPreLRMSWaitingJobs(getAttributeValueAsUnsignedInt(attributes.get("GLUE2EndpointPreLRMSWaitingJobs")));
                computingEndpoint.setQualityLevel(getAttributeValueAsQualityLevel(attributes.get("GLUE2EndpointQualityLevel")));
                computingEndpoint.setRunningJobs(getAttributeValueAsUnsignedInt(attributes.get("GLUE2ComputingEndpointRunningJobs")));
                computingEndpoint.setSemantics(getAttributeValueAsURIArray(attributes.get("GLUE2EndpointSemantics")));
                computingEndpoint.setServingState(getAttributeValueAsServingState(attributes.get("GLUE2EndpointServingState")));
                computingEndpoint.setStaging(getAttributeValueAsStaging(attributes.get("GLUE2ComputingEndpointStaging")));
                computingEndpoint.setStagingJobs(getAttributeValueAsUnsignedInt(attributes.get("GLUE2ComputingEndpointStagingJobs")));
                computingEndpoint.setStartTime(getAttributeValueAsDateTime(attributes.get("GLUE2EndpointStartTime")));
                computingEndpoint.setSupportedProfile(getAttributeValueAsURIArray(attributes.get("GLUE2EndpointSupportedProfile")));
                computingEndpoint.setSuspendedJobs(getAttributeValueAsUnsignedInt(attributes.get("GLUE2ComputingEndpointSuspendedJobs")));
                computingEndpoint.setTechnology(getAttributeValueAsEndpointTechnology(attributes.get("GLUE2EndpointTechnology")));
                computingEndpoint.setTotalJobs(getAttributeValueAsUnsignedInt(attributes.get("GLUE2ComputingEndpointTotalJobs")));
                computingEndpoint.setTrustedCA(getAttributeValueAsDNArray(attributes.get("GLUE2EndpointTrustedCA")));
                computingEndpoint.setURL(getAttributeValueAsURI(attributes.get("GLUE2EndpointURL")));
                computingEndpoint.setValidity(getAttributeValueAsUnsignedLong(attributes.get("GLUE2EntityValidity")));
                computingEndpoint.setWaitingJobs(getAttributeValueAsUnsignedInt(attributes.get("GLUE2ComputingEndpointWaitingJobs")));
                computingEndpoint.setWSDL(getAttributeValueAsURIArray(attributes.get("GLUE2EndpointWSDL")));
                computingEndpoint.setAccessPolicy(makeAccessPolicies(ctx, computingEndpoint.getID().toString()));

                computingEndpoints.add(computingEndpoint);
            }
        }

        ComputingEndpoint_t[] result = null;

        if (computingEndpoints.size() > 0) {
            result = new ComputingEndpoint_t[computingEndpoints.size()];
            result = computingEndpoints.toArray(result);
        }

        return result;
    }

    private ComputingManager_t[] makeComputingManagers(DirContext ctx, String computingServiceId) throws NamingException {
        Attributes attributes = null;
        SearchResult searchResult = null;
        SearchControls ctls = new SearchControls();
        ctls.setSearchScope(SearchControls.SUBTREE_SCOPE);
        NamingEnumeration<SearchResult> answer = ctx.search("o=glue", "(&(GLUE2ComputingManagerComputingServiceForeignKey=" + computingServiceId
                + ")(objectclass=GLUE2ComputingManager))", ctls);
        ComputingManager_t computingManager = null;
        List<ComputingManager_t> computingManagers = new ArrayList<ComputingManager_t>(0);

        while (answer.hasMoreElements()) {
            searchResult = answer.nextElement();
            // System.out.println(searchResult.getNameInNamespace());

            attributes = searchResult.getAttributes();

            if (attributes != null) {
                // printAttributes(attributes);

                computingManager = new ComputingManager_t();
                computingManager.setID(getAttributeValueAsURI(attributes.get("GLUE2ManagerID")));
                computingManager.setApplicationDir(getAttributeValueAsString(attributes.get("GLUE2ComputingManagerApplicationDir")));
                ApplicationEnvironments_type0 applicationEnvironments = new ApplicationEnvironments_type0();
                applicationEnvironments.setApplicationEnvironment(makeApplicationEnvironments(ctx, computingManager.getID().toString()));
                computingManager.setApplicationEnvironments(applicationEnvironments);
                computingManager.setBenchmark(makeBenchmarks(ctx, computingManager.getID().toString()));
                computingManager.setBulkSubmission(getAttributeValueAsExtendedBoolean(attributes.get("GLUE2ComputingManagerBulkSubmission")));
                computingManager.setCacheFree(getAttributeValueAsUnsignedLong(attributes.get("GLUE2ComputingManagerCacheFree")));
                computingManager.setCacheTotal(getAttributeValueAsUnsignedLong(attributes.get("GLUE2ComputingManagerCacheTotal")));
                computingManager.setCreationTime(getAttributeValueAsDateTime(attributes.get("GLUE2EntityCreationTime")));
                // computingManager.setExecutionEnvironments(param);
                // computingManager.setExtensions(param);
                computingManager.setHomogeneous(getAttributeValueAsExtendedBoolean(attributes.get("GLUE2ComputingManagerHomogeneous")));
                computingManager.setLogicalCPUDistribution(getAttributeValueAsString(attributes.get("GLUE2ComputingManagerLogicalCPUDistribution")));
                computingManager.setName(getAttributeValueAsString(attributes.get("GLUE2EntityName")));
                // computingManager.setNetworkInfo(param);
                computingManager.setOtherInfo(getAttributeValueAsStringArray(attributes.get("GLUE2EntityOtherInfo")));
                computingManager.setProductName(getAttributeValueAsComputingManagerType(attributes.get("GLUE2ManagerProductName")));
                computingManager.setProductVersion(getAttributeValueAsString(attributes.get("GLUE2ManagerProductVersion")));
                // computingManager.setReservation(param);
                computingManager.setScratchDir(getAttributeValueAsString(attributes.get("GLUE2ComputingManagerScratchDir")));
                computingManager.setSlotsUsedByGridJobs(getAttributeValueAsUnsignedInt(attributes.get("GLUE2ComputingManagerSlotsUsedByGridJobs")));
                computingManager.setSlotsUsedByLocalJobs(getAttributeValueAsUnsignedInt(attributes.get("GLUE2ComputingManagerSlotsUsedByLocalJobs")));
                computingManager.setTmpDir(getAttributeValueAsString(attributes.get("GLUE2ComputingManagerTmpDir")));
                computingManager.setTotalLogicalCPUs(getAttributeValueAsUnsignedInt(attributes.get("GLUE2ComputingManagerTotalLogicalCPUs")));
                computingManager.setTotalPhysicalCPUs(getAttributeValueAsUnsignedInt(attributes.get("GLUE2ComputingManagerTotalPhysicalCPUs")));
                computingManager.setTotalSlots(getAttributeValueAsUnsignedInt(attributes.get("GLUE2ComputingManagerTotalSlots")));
                computingManager.setValidity(getAttributeValueAsUnsignedLong(attributes.get("GLUE2EntityValidity")));
                computingManager.setWorkingAreaFree(getAttributeValueAsUnsignedLong(attributes.get("GLUE2ComputingManagerWorkingAreaFree")));
                computingManager.setWorkingAreaGuaranteed(getAttributeValueAsExtendedBoolean(attributes.get("GLUE2ComputingManagerWorkingAreaGuaranteed")));
                computingManager.setWorkingAreaLifeTime(getAttributeValueAsUnsignedLong(attributes.get("GLUE2ComputingManagerWorkingAreaLifeTime")));
                computingManager.setWorkingAreaMultiSlotFree(getAttributeValueAsUnsignedLong(attributes.get("GLUE2ComputingManagerWorkingAreaMultiSlotFree")));
                computingManager.setWorkingAreaMultiSlotLifeTime(getAttributeValueAsUnsignedLong(attributes.get("GLUE2ComputingManagerWorkingMultiSlotLifeTime")));
                computingManager.setWorkingAreaMultiSlotTotal(getAttributeValueAsUnsignedLong(attributes.get("GLUE2ComputingManagerWorkingMultiSlotTotal")));
                computingManager.setWorkingAreaShared(getAttributeValueAsExtendedBoolean(attributes.get("GLUE2ComputingManagerWorkingAreaShared")));
                computingManager.setWorkingAreaTotal(getAttributeValueAsUnsignedLong(attributes.get("GLUE2ComputingManagerWorkingAreaTotal")));

                computingManagers.add(computingManager);
            }
        }

        ComputingManager_t[] result = null;

        if (computingManagers.size() > 0) {
            result = new ComputingManager_t[computingManagers.size()];
            result = computingManagers.toArray(result);
        }

        return result;
    }

    private ComputingService_t makeComputingService(DirContext ctx) throws NamingException {
        Attributes attributes = null;
        SearchResult searchResult = null;
        SearchControls ctls = new SearchControls();
        ctls.setSearchScope(SearchControls.SUBTREE_SCOPE);
        NamingEnumeration<SearchResult> answer = ctx.search("o=glue", "(&(objectClass=GLUE2ComputingService)(GLUE2ServiceType=org.ogf.emies))", ctls);
        ComputingService_t computingService = null;

        if (answer.hasMoreElements()) {
           computingService = new ComputingService_t();
        }

        while (answer.hasMoreElements()) {
            searchResult = answer.nextElement();
            // System.out.println(searchResult.getNameInNamespace());

            attributes = searchResult.getAttributes();

            if (attributes != null) {
                // printAttributes(attributes);

                // computingService.setAssociations(param);
                computingService.setID(getAttributeValueAsURI(attributes.get("GLUE2ServiceID")));
                computingService.setCapability(getAttributeValueAsCapability(attributes.get("GLUE2ServiceCapability")));
                computingService.setComplexity(getAttributeValueAsString(attributes.get("GLUE2ServiceComplexity")));
                computingService.setComputingEndpoint(makeComputingEndpoints(ctx, computingService.getID().toString()));
                computingService.setComputingManager(makeComputingManagers(ctx, computingService.getID().toString()));
                computingService.setComputingShare(makeComputingShares(ctx, computingService.getID().toString()));
                // computingService.setContact(param);
                computingService.setCreationTime(getAttributeValueAsDateTime(attributes.get("GLUE2EntityCreationTime")));
                // computingService.setExtensions(param);
                // computingService.setLocation(param);
                computingService.setName(getAttributeValueAsString(attributes.get("GLUE2EntityName")));
                computingService.setOtherInfo(getAttributeValueAsStringArray(attributes.get("GLUE2EntityOtherInfo")));
                computingService.setPreLRMSWaitingJobs(getAttributeValueAsUnsignedInt(attributes.get("GLUE2ComputingServicePreLRMSWaitingJobs")));
                computingService.setQualityLevel(getAttributeValueAsQualityLevel(attributes.get("GLUE2ServiceQualityLevel")));
                computingService.setRunningJobs(getAttributeValueAsUnsignedInt(attributes.get("GLUE2ComputingServiceRunningJobs")));
                computingService.setStagingJobs(getAttributeValueAsUnsignedInt(attributes.get("GLUE2ComputingServiceStagingJobs")));
                computingService.setStatusInfo(getAttributeValueAsURIArray(attributes.get("GLUE2ServiceStatusInfo")));
                computingService.setSuspendedJobs(getAttributeValueAsUnsignedInt(attributes.get("GLUE2ComputingServiceSuspendedJobs")));
                computingService.setToStorageService(makeToStorageService(ctx, computingService.getID().toString()));
                computingService.setTotalJobs(getAttributeValueAsUnsignedInt(attributes.get("GLUE2ComputingServiceTotalJobs")));
                computingService.setType(getAttributeValueAsServiceType(attributes.get("GLUE2ServiceType")));
                computingService.setValidity(getAttributeValueAsUnsignedLong(attributes.get("GLUE2EntityValidity")));
                computingService.setWaitingJobs(getAttributeValueAsUnsignedInt(attributes.get("GLUE2ComputingServiceWaitingJobs")));
            }
        }

        return computingService;
    }

    private ComputingShare_t[] makeComputingShares(DirContext ctx, String computingServiceId) throws NamingException {
        Attributes attributes = null;
        SearchResult searchResult = null;
        SearchControls ctls = new SearchControls();
        ctls.setSearchScope(SearchControls.SUBTREE_SCOPE);
        NamingEnumeration<SearchResult> answer = ctx.search("o=glue", "(&(GLUE2ComputingShareComputingServiceForeignKey=" + computingServiceId
                + ")(objectclass=GLUE2ComputingShare))", ctls);
        ComputingShare_t computingShare = null;
        List<ComputingShare_t> computingShares = new ArrayList<ComputingShare_t>(0);

        while (answer.hasMoreElements()) {
            searchResult = answer.nextElement();
            // System.out.println(searchResult.getNameInNamespace());

            attributes = searchResult.getAttributes();

            if (attributes != null) {
                // printAttributes(attributes);

                computingShare = new ComputingShare_t();
                // computingShare.setAssociations(param);
                computingShare.setCreationTime(getAttributeValueAsDateTime(attributes.get("GLUE2EntityCreationTime")));
                computingShare.setDefaultCPUTime(getAttributeValueAsUnsignedLong(attributes.get("GLUE2ComputingShareDefaultCPUTime")));
                computingShare.setDefaultStorageService(getAttributeValueAsURI(attributes.get("GLUE2ComputingShareDefaultStorageService")));
                computingShare.setDefaultWallTime(getAttributeValueAsUnsignedLong(attributes.get("GLUE2ComputingShareDefaultWallTime")));
                computingShare.setDescription(getAttributeValueAsString(attributes.get("GLUE2ShareDescription")));
                computingShare.setEstimatedAverageWaitingTime(getAttributeValueAsUnsignedLong(attributes.get("GLUE2ComputingShareEstimatedAverageWaitingTime")));
                computingShare.setEstimatedWorstWaitingTime(getAttributeValueAsUnsignedLong(attributes.get("GLUE2ComputingShareEstimatedWorstWaitingTime")));
                // computingShare.setExtensions(param);
                computingShare.setFreeSlots(getAttributeValueAsUnsignedInt(attributes.get("GLUE2ComputingShareFreeSlots")));
                computingShare.setFreeSlotsWithDuration(getAttributeValueAsString(attributes.get("GLUE2ComputingShareFreeSlotsWithDuration")));
                computingShare.setGuaranteedMainMemory(getAttributeValueAsUnsignedLong(attributes.get("GLUE2ComputingShareGuaranteedMainMemory")));
                computingShare.setGuaranteedVirtualMemory(getAttributeValueAsUnsignedLong(attributes.get("GLUE2ComputingShareGuaranteedVirtualMemory")));
                computingShare.setID(getAttributeValueAsURI(attributes.get("GLUE2ShareID")));
                computingShare.setLocalRunningJobs(getAttributeValueAsUnsignedInt(attributes.get("GLUE2ComputingShareLocalRunningJobs")));
                computingShare.setLocalSuspendedJobs(getAttributeValueAsUnsignedInt(attributes.get("GLUE2ComputingShareLocalSuspendedJobs")));
                computingShare.setLocalWaitingJobs(getAttributeValueAsUnsignedInt(attributes.get("GLUE2ComputingShareLocalWaitingJobs")));
                computingShare.setMappingPolicy(makeMappingPolicies(ctx, computingShare.getID().toString()));
                computingShare.setMappingQueue(getAttributeValueAsString(attributes.get("GLUE2ComputingShareMappingQueue")));
                computingShare.setMaxCPUTime(getAttributeValueAsUnsignedLong(attributes.get("GLUE2ComputingShareMaxCPUTime")));
                computingShare.setMaxDiskSpace(getAttributeValueAsUnsignedLong(attributes.get("GLUE2ComputingShareMaxDiskSpace")));
                computingShare.setMaxMainMemory(getAttributeValueAsUnsignedLong(attributes.get("GLUE2ComputingShareMaxMainMemory")));
                computingShare.setMaxMultiSlotWallTime(getAttributeValueAsUnsignedLong(attributes.get("GLUE2ComputingShareMaxMultiSlotWallTime")));
                computingShare.setMaxPreLRMSWaitingJobs(getAttributeValueAsUnsignedInt(attributes.get("GLUE2ComputingShareMaxPreLRMSWaitingJobs")));
                computingShare.setMaxRunningJobs(getAttributeValueAsUnsignedInt(attributes.get("GLUE2ComputingShareMaxRunningJobs")));
                computingShare.setMaxSlotsPerJob(getAttributeValueAsUnsignedInt(attributes.get("GLUE2ComputingShareMaxSlotsPerJob")));
                computingShare.setMaxStageOutStreams(getAttributeValueAsUnsignedInt(attributes.get("GLUE2ComputingShareMaxStageOutStreams")));
                computingShare.setMaxStateInStreams(getAttributeValueAsUnsignedInt(attributes.get("GLUE2ComputingShareMaxStateInStreams")));
                computingShare.setMaxTotalCPUTime(getAttributeValueAsUnsignedLong(attributes.get("GLUE2ComputingShareMaxTotalCPUTime")));
                computingShare.setMaxTotalJobs(getAttributeValueAsUnsignedInt(attributes.get("GLUE2ComputingShareMaxTotalJobs")));
                computingShare.setMaxUserRunningJobs(getAttributeValueAsUnsignedInt(attributes.get("GLUE2ComputingShareMaxUserRunningJobs")));
                computingShare.setMaxVirtualMemory(getAttributeValueAsUnsignedLong(attributes.get("GLUE2ComputingShareMaxVirtualMemory")));
                computingShare.setMaxWaitingJobs(getAttributeValueAsUnsignedInt(attributes.get("GLUE2ComputingShareMaxWaitingJobs")));
                computingShare.setMaxWallTime(getAttributeValueAsUnsignedLong(attributes.get("GLUE2ComputingShareMaxWallTime")));
                computingShare.setMinCPUTime(getAttributeValueAsUnsignedLong(attributes.get("GLUE2ComputingShareMinCPUTime")));
                computingShare.setMinWallTime(getAttributeValueAsUnsignedLong(attributes.get("GLUE2ComputingShareMinWallTime")));
                computingShare.setName(getAttributeValueAsString(attributes.get("GLUE2EntityName")));
                computingShare.setOtherInfo(getAttributeValueAsStringArray(attributes.get("GLUE2EntityOtherInfo")));
                computingShare.setPreemption(getAttributeValueAsExtendedBoolean(attributes.get("GLUE2ComputingSharePreemption")));
                computingShare.setPreLRMSWaitingJobs(getAttributeValueAsUnsignedInt(attributes.get("GLUE2ComputingSharePreLRMSWaitingJobs")));
                computingShare.setRequestedSlots(getAttributeValueAsUnsignedInt(attributes.get("GLUE2ComputingShareRequestedSlots")));
                computingShare.setReservationPolicy(getAttributeValueAsReservationPolicy(attributes.get("GLUE2ComputingShareReservationPolicy")));
                computingShare.setRunningJobs(getAttributeValueAsUnsignedInt(attributes.get("GLUE2ComputingShareRunningJobs")));
                computingShare.setSchedulingPolicy(getAttributeValueAsSchedulingPolicy(attributes.get("GLUE2ComputingShareSchedulingPolicy")));
                computingShare.setServingState(getAttributeValueAsServingState(attributes.get("GLUE2ComputingShareServingState")));
                computingShare.setStagingJobs(getAttributeValueAsUnsignedInt(attributes.get("GLUE2ComputingShareStagingJobs")));
                computingShare.setSuspendedJobs(getAttributeValueAsUnsignedInt(attributes.get("GLUE2ComputingShareSuspendedJobs")));
                computingShare.setTag(getAttributeValueAsStringArray(attributes.get("GLUE2ComputingShareTag")));
                computingShare.setTotalJobs(getAttributeValueAsUnsignedInt(attributes.get("GLUE2ComputingShareTotalJobs")));
                computingShare.setUsedSlots(getAttributeValueAsUnsignedInt(attributes.get("GLUE2ComputingShareUsedSlots")));
                computingShare.setValidity(getAttributeValueAsUnsignedLong(attributes.get("GLUE2EntityValidity")));
                computingShare.setWaitingJobs(getAttributeValueAsUnsignedInt(attributes.get("GLUE2ComputingShareWaitingJobs")));

                computingShares.add(computingShare);
            }
        }

        ComputingShare_t[] result = null;

        if (computingShares.size() > 0) {
            result = new ComputingShare_t[computingShares.size()];
            result = computingShares.toArray(result);
        }

        return result;
    }

    private MappingPolicy_t[] makeMappingPolicies(DirContext ctx, String computingShareId) throws NamingException {
        Attributes attributes = null;
        SearchResult searchResult = null;
        SearchControls ctls = new SearchControls();
        ctls.setSearchScope(SearchControls.SUBTREE_SCOPE);
        NamingEnumeration<SearchResult> answer = ctx.search("o=glue", "(&(GLUE2MappingPolicyShareForeignKey=" + computingShareId + ")(objectclass=GLUE2MappingPolicy))", ctls);
        MappingPolicy_t mappingPolicie = null;
        List<MappingPolicy_t> mappingPolicies = new ArrayList<MappingPolicy_t>(0);

        while (answer.hasMoreElements()) {
            searchResult = answer.nextElement();
            // System.out.println(searchResult.getNameInNamespace());

            attributes = searchResult.getAttributes();

            if (attributes != null) {
                // printAttributes(attributes);

                mappingPolicie = new MappingPolicy_t();
                // mappingPolicie.setAssociations(param);
                mappingPolicie.setCreationTime(getAttributeValueAsDateTime(attributes.get("GLUE2EntityCreationTime")));
                // mappingPolicie.setExtensions(param);
                mappingPolicie.setID(getAttributeValueAsURI(attributes.get("GLUE2PolicyID")));
                mappingPolicie.setName(getAttributeValueAsString(attributes.get("GLUE2EntityName")));
                mappingPolicie.setOtherInfo(getAttributeValueAsStringArray(attributes.get("GLUE2EntityOtherInfo")));
                mappingPolicie.setRule(getAttributeValueAsStringArray(attributes.get("GLUE2PolicyRule")));
                mappingPolicie.setScheme(getAttributeValueAsPolicyScheme(attributes.get("GLUE2PolicyScheme")));
                mappingPolicie.setValidity(getAttributeValueAsUnsignedLong(attributes.get("GLUE2EntityValidity")));

                mappingPolicies.add(mappingPolicie);
            }
        }

        MappingPolicy_t[] result = null;

        if (mappingPolicies.size() > 0) {
            result = new MappingPolicy_t[mappingPolicies.size()];
            result = mappingPolicies.toArray(result);
        }

        return result;
    }

    private ToStorageService_t makeToStorageService(DirContext ctx, String computingServiceId) throws NamingException {
        Attributes attributes = null;
        SearchResult searchResult = null;
        SearchControls ctls = new SearchControls();
        ctls.setSearchScope(SearchControls.SUBTREE_SCOPE);
        NamingEnumeration<SearchResult> answer = ctx.search("o=glue", "(&(GLUE2ToStorageServiceComputingServiceForeignKey=" + computingServiceId
                + ")(objectclass=GLUE2ToStorageService))", ctls);
        ToStorageService_t toStorageService = null;
        List<ToStorageService_t> computingShares = new ArrayList<ToStorageService_t>(0);

        if (answer.hasMoreElements()) {
            searchResult = answer.nextElement();
            // System.out.println(searchResult.getNameInNamespace());

            attributes = searchResult.getAttributes();

            if (attributes != null) {
                // printAttributes(attributes);

                toStorageService = new ToStorageService_t();
                // toStorageService.setAssociations(param);
                toStorageService.setCreationTime(getAttributeValueAsDateTime(attributes.get("GLUE2EntityCreationTime")));
                // toStorageService.setExtensions(param);
                toStorageService.setID(getAttributeValueAsURI(attributes.get("GLUE2ToStorageServiceID")));
                toStorageService.setLocalPath(getAttributeValueAsString(attributes.get("GLUE2ToStorageServiceLocalPath")));
                toStorageService.setName(getAttributeValueAsString(attributes.get("GLUE2EntityName")));
                toStorageService.setOtherInfo(getAttributeValueAsStringArray(attributes.get("GLUE2EntityOtherInfo")));
                toStorageService.setRemotePath(getAttributeValueAsString(attributes.get("GLUE2ToStorageServiceRemotePath")));
                toStorageService.setValidity(getAttributeValueAsUnsignedLong(attributes.get("GLUE2EntityValidity")));

                Associations_type12 associations = new Associations_type12();
                associations.setStorageServiceID(getAttributeValueAsURI(attributes.get("GLUE2ToStorageServiceStorageServiceForeignKey")));
                toStorageService.setAssociations(associations);
            }
        }

        return toStorageService;
    }

    private void printAttributes(Attributes attributes) {
        Attribute attribute = null;
        NamingEnumeration<? extends Attribute> attr = attributes.getAll();

        while (attr.hasMoreElements()) {
            attribute = attr.nextElement();
            try {
                for (NamingEnumeration vals = attribute.getAll(); vals.hasMoreElements(); System.out.println("\t" + attribute.getID() + " = " + vals.nextElement()))
                    ;
            } catch (NamingException e) {
                e.printStackTrace();
            }
        }
    }

    public void run() {
        logger.debug("BEGIN GLUE2Handler");
        ComputingService_t service = null;

        while (!terminate) {
            if (ctx != null) {
                try {
                    logger.debug("making the ComputingService_t...");
                    service = makeComputingService(ctx);
                    logger.debug("making the ComputingService_t... done");
                } catch (Throwable t) {
                    logger.error(t.getMessage());
                }
            }

            if (computingService == null) {
                computingService = service;
            }

            if (service == null) {
                logger.warn("ComputingService_t not available!");
            } else {
                synchronized (computingService) {
                    computingService = service;

                    try {
                        String ns = "xmlns=\"http://schemas.ogf.org/glue/2009/03/spec_2.0_r1\"";
                        String xmlStream = computingService.getOMElement(ComputingService.MY_QNAME, OMAbstractFactory.getOMFactory()).toString();

                        int index = xmlStream.indexOf(ns);
                        if (index > 0) {
                            xmlStream = xmlStream.substring(0, index) + xmlStream.substring(index + 1);
                        }

                        StAXBuilder builder = new StAXOMBuilder(new ByteArrayInputStream(xmlStream.getBytes()));
                        computingServiceElement = builder.getDocumentElement();
                    } catch (Throwable t) {
                        logger.error(t.getMessage());
                    }
                }
            }
 
            synchronized(this) {
                try {
                    logger.debug("waiting " + rate + " minutes...");
                    wait(rate);
                    logger.debug("waiting " + rate + " minutes... done!");
                } catch(InterruptedException e) {
                    terminate = true;
                }
            }
        }

        try {
            ctx.close();
        } catch (Throwable t) {
            logger.error(t.getMessage());
        }

        logger.debug("END GLUE2Handler");
    }

    public void terminate() {
        logger.info("teminate invoked!");
        terminate = true;

        synchronized(this) {
            notifyAll();
        }

        logger.info("teminated!");
    }
}

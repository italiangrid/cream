package org.glite.ce.cream.configuration;

import java.util.ArrayList;
import java.util.List;

import org.glite.ce.commonj.configuration.CEConfigResource;
import org.glite.ce.creamapi.cmdmanagement.Parameter;
import org.glite.ce.creamapi.cmdmanagement.Policy;

public class CommandExecutorConfig implements CEConfigResource {
    private static final long serialVersionUID = 1L;
    protected String name;
    protected String category;
    protected String jarFileName;
    protected int commandWorkerPoolSize = 50;
    protected int commandQueueSize = 500;
    protected boolean isCommandQueueShared = false;
    protected List<Parameter> parameters;
    protected List<Policy> policies;

    public CommandExecutorConfig() {
        parameters = new ArrayList<Parameter>(0);
        policies = new ArrayList<Policy>(0);
    }

    public void addParameter(Parameter parameter) {
        if (parameter == null) {
            return;
        }
        parameters.add(parameter);
    }

    public void addPolicy(Policy policy) {
        if (policy == null) {
            return;
        }
        policies.add(policy);
    }

    public Object clone() {
        CommandExecutorConfig result = new CommandExecutorConfig();
        result.setName(name);
        result.setCategory(category);
        result.setJarFileName(jarFileName);
        result.setCommandWorkerPoolSize(commandWorkerPoolSize);
        result.setCommandQueueSize(commandQueueSize);
        result.setCommandQueueShared(isCommandQueueShared);

        for (Parameter parameter : parameters) {
            result.addParameter(new Parameter(parameter.getName(), parameter.getValue()));
        }

        for (Policy policy : policies) {
            result.addPolicy(new Policy(policy.getName(), policy.getType(), policy.getValue(), policy.getTimeValue(), policy.getTimeUnit()));
        }

        return result;
    }
    
    public boolean equals(Object obj) {
        if (obj instanceof CommandExecutorConfig) {
            return false;
        }
        
        CommandExecutorConfig cmdExConf = (CommandExecutorConfig) obj;
        if (!name.equals(cmdExConf.name) ||
                !category.equals(cmdExConf.category) ||
                !jarFileName.equals(cmdExConf.jarFileName) ||
                commandWorkerPoolSize!=cmdExConf.commandWorkerPoolSize ||
                commandQueueSize!=cmdExConf.commandQueueSize ||
                isCommandQueueShared^cmdExConf.isCommandQueueShared ||
                !parameters.equals(cmdExConf.parameters) ||
                !policies.equals(cmdExConf.policies)) {
            return false;
        }
        
        return true;
    }

    public String getCategory() {
        return category;
    }

    public int getCommandQueueSize() {
        return commandQueueSize;
    }

    public int getCommandWorkerPollSize() {
        return commandWorkerPoolSize;
    }

    public String getJarFileName() {
        return jarFileName;
    }

    public String getName() {
        return name;
    }

    public List<Parameter> getParameters() {
        return parameters;
    }

    public List<Policy> getPolicies() {
        return policies;
    }

    public boolean isCommandQueueShared() {
        return isCommandQueueShared;
    }

    public void setCategory(String category) {
        this.category = category;
    }
    
    public void setCommandQueueShared(boolean isShared) {
        isCommandQueueShared = isShared;
    }
                
    public void setCommandQueueSize(int size) {
        commandQueueSize = size;
    }

    public void setCommandWorkerPoolSize(int commandWorkerPollSize) {        
        this.commandWorkerPoolSize = commandWorkerPollSize;
    }

    public void setJarFileName(String jarFileName) {
        this.jarFileName = jarFileName;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setParameters(List<Parameter> parameters) {
        this.parameters.clear();
        this.parameters.addAll(parameters);
    }

    public void setPolicies(List<Policy> policies) {
        this.policies.clear();
        this.policies.addAll(policies);
    }
}

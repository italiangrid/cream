package org.glite.ce.cream.cmdmanagement;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Hashtable;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.glite.ce.commonj.utils.JarClassLoader;
import org.glite.ce.cream.configuration.CommandExecutorConfig;
import org.glite.ce.cream.configuration.ServiceConfig;
import org.glite.ce.creamapi.cmdmanagement.Command;
import org.glite.ce.creamapi.cmdmanagement.CommandException;
import org.glite.ce.creamapi.cmdmanagement.CommandExecutorException;
import org.glite.ce.creamapi.cmdmanagement.CommandExecutorInterface;
import org.glite.ce.creamapi.cmdmanagement.CommandManagerException;
import org.glite.ce.creamapi.cmdmanagement.CommandManagerInterface;
import org.glite.ce.creamapi.cmdmanagement.queue.CommandQueueInterface;

public class CommandManager extends Thread implements CommandManagerInterface {
    private static final Logger logger = Logger.getLogger(CommandManager.class.getName());
    private static final int EVALUATION_RATE = 600000; //1 hour
    private static final int INITIALIZATION_TBD = 0;
    private static final int INITIALIZATION_OK = 1;
    private static final int INITIALIZATION_ERROR = 2;
    private static int initialization = INITIALIZATION_TBD;
    private static boolean exit = false;
    private static CommandManager commandManager = null;

    public static CommandManager getInstance() throws CommandManagerException {
        if (exit) {
            throw new CommandManagerException("CommandExecutor already terminated!");
        }

        if (initialization == INITIALIZATION_ERROR) {
            throw new CommandManagerException("CommandManager configuration failed!");
        }

        if (commandManager == null) {
            commandManager = new CommandManager();
        }

        return commandManager;
    }

    private final Object lock = new Object();
    private final AtomicLong maxThroughput = new AtomicLong(0);
    private final AtomicLong currentThroughput = new AtomicLong(0);
    private final AtomicLong commandsCount = new AtomicLong(0);
    private Calendar lastThroughputUpdate = null;
    private Hashtable<String, Hashtable<String, CommandExecutorInterface>> commandExecutorCategory;

    private CommandManager() throws CommandManagerException {
        super("CommandManager");

        init();
        start();
    }

    public void addCommandExecutor(CommandExecutorConfig commandExecutorConfig) throws CommandManagerException {
        if (commandExecutorConfig == null) {
            throw new CommandManagerException("CommandExecutorConfig not specified!");
        }

        Hashtable<String, CommandExecutorInterface> executorTable = commandExecutorCategory.get(commandExecutorConfig.getCategory());

        if (executorTable != null && executorTable.containsKey(commandExecutorConfig.getName())) {
            logger.debug("CommandExecutor \"" + commandExecutorConfig.getName() + "\" already exists");
            throw new CommandManagerException("CommandExecutor \"" + commandExecutorConfig.getName() + "\" already exists");
        }

        String jarFilePath = null;
        Class<CommandExecutorInterface> theClass = null;
        Object obj = null;
        try {
            jarFilePath = commandExecutorConfig.getJarFileName();

            logger.info("initializing a new CommandExecutor [name=" + commandExecutorConfig.getName() + "; category=" + commandExecutorConfig.getCategory()
                    + "; JarFileName=" + commandExecutorConfig.getJarFileName() + "]");

            File jarFile = new File(jarFilePath);
            try {
                if (!jarFile.getName().endsWith(".jar")) {
                    throw (new RuntimeException("Cannot create a CommandExecutor instance from file \"" + jarFile.getAbsolutePath() + "\""));
                }

                URL commandExecutorURL = jarFile.toURI().toURL();

                JarClassLoader loader = new JarClassLoader("file:" + jarFile.getPath(), this.getClass().getClassLoader());
                loader.addJarURL(commandExecutorURL);

                String className = loader.getMainClassName(commandExecutorURL);
                if (className != null) {
                    theClass = (Class<CommandExecutorInterface>) loader.loadClass(className);
                } else {
                    throw (new CommandManagerException("\"Main-Class\" attribute not found into the MANIFEST.MF of " + jarFile.getAbsolutePath() + ".jar"));
                }
            } catch (Exception e) {
                throw new CommandManagerException("Cannot create the CommandExecutor class from file " + jarFile.getName() + ". Reason: " + jarFilePath + " " + e.getMessage());
            }

            if (theClass == null) {
                throw new RuntimeException("Cannot create the CommandExecutor class from path " + jarFilePath);
            }

            obj = theClass.newInstance();

            if (obj instanceof CommandExecutorInterface) {
                CommandExecutorInterface commandExecutor = (CommandExecutorInterface) obj;
                commandExecutor.addParameter(commandExecutorConfig.getParameters());
                commandExecutor.setCommandWorkerPoolSize(commandExecutorConfig.getCommandWorkerPollSize());
                commandExecutor.setCommandQueueSize(commandExecutorConfig.getCommandQueueSize());
                commandExecutor.setCommandQueueShared(commandExecutorConfig.isCommandQueueShared());

                logger.info("New CommandExecutor found! [name=" + commandExecutorConfig.getName() +
                        "; category=" + commandExecutorConfig.getCategory() + 
                        "; commandWorkerPoolSize=" + commandExecutorConfig.getCommandWorkerPollSize() + 
                        "; commandQueueSize=" + commandExecutorConfig.getCommandQueueSize() + 
                        "; commandQueueShared=" + commandExecutorConfig.isCommandQueueShared() +
                        "; parameterSize=" + commandExecutorConfig.getParameters().size() + "]");

                addCommandExecutor(commandExecutor);
            }
        } catch (CommandManagerException e) {
            logger.error("initialization error: " + e.getMessage());
            throw e;
        } catch (IllegalAccessException e) {
            logger.error("initialization error: " + e.getMessage());
            throw new CommandManagerException(e.getMessage());
        } catch (InstantiationException e) {
            logger.error("initialization error: cannot make an instance of " + jarFilePath + " commandExecutor! reason " + e.getMessage());
            throw new CommandManagerException("initialization error: cannot make an instance of " + jarFilePath + " commandExecutor! reason " + e.getMessage());
        }
    }

    public void addCommandExecutor(CommandExecutorInterface commandExecutor) throws CommandManagerException {
        if (commandExecutor == null) {
            throw new CommandManagerException("CommandExecutor not specified!");
        }

        Hashtable<String, CommandExecutorInterface> executorTable = commandExecutorCategory.get(commandExecutor.getCategory());

        if (executorTable == null) {
            executorTable = new Hashtable<String, CommandExecutorInterface>(0);
            commandExecutorCategory.put(commandExecutor.getCategory(), executorTable);
        }

        if (executorTable.containsKey(commandExecutor.getName())) {
            logger.debug("CommandExecutor \"" + commandExecutor.getName() + "\" already exists");
            throw new CommandManagerException("CommandExecutor \"" + commandExecutor.getName() + "\" already exists");
        }

        try {
            commandExecutor.setCommandManager(this);
            commandExecutor.initExecutor();
            commandExecutor.start();
            executorTable.put(commandExecutor.getName(), commandExecutor);
            
//            if (getState().compareTo(State.NEW) == 0) {
//                start();                
//            }
            logger.debug("New CommandExecutor \"" + commandExecutor.getName() + "\" added");
        } catch (CommandExecutorException e) {
            logger.error("CommandExecutor \"" + commandExecutor.getName() + "\" initialization failed: " + e.getMessage());
            throw new CommandManagerException("CommandExecutor \"" + commandExecutor.getName() + "\" initialization failed: " + e.getMessage());
        }
    }

    public void addCommandExecutor(List<CommandExecutorInterface> commandExecList) throws CommandManagerException {
        if (commandExecList == null) {
            throw new CommandManagerException("CommandExecutorInterface list not specified!");
        }

        for (CommandExecutorInterface commandExecutor : commandExecList) {
            addCommandExecutor(commandExecutor);
        }
    }
    
    public boolean checkCommandExecutor(String name, String category) {
        if (name == null) {
            return false;
        }
        
        if (category == null) {
            return false;
        }
        
        Hashtable<String, CommandExecutorInterface> executorTable = commandExecutorCategory.get(category);

        return (executorTable != null && executorTable.containsKey(name));
    }

    public void execute(Command command) throws CommandException, CommandManagerException {
        logger.debug("BEGIN execute");
        if (command == null) {
            throw new CommandException("command not specified!");
        }

        synchronized (lastThroughputUpdate) {
            commandsCount.incrementAndGet();
        }

        if (command.getCategory() == null) {
            throw new CommandException("command category not defined!");
        }

        Hashtable<String, CommandExecutorInterface> executorTable = commandExecutorCategory.get(command.getCategory());

        if (executorTable == null) {
            throw new CommandException("command category \"" + command.getCategory() + "\" not found!");
        }

        CommandExecutorInterface executor = null;

        if (command.getCommandExecutorName() == null) {
            Collection<CommandExecutorInterface> commandExecutor = executorTable.values();
            ArrayList<CommandExecutorInterface> list = new ArrayList<CommandExecutorInterface>(commandExecutor);
            int index = 0;
            while (executor == null && index < list.size()) {
                executor = list.get(index++);
            }

            if (executor == null) {
                throw new CommandManagerException("none right executor can handle the command \"" + command.getName() + "\"");
            }
            
            command.setCommandExecutorName(executor.getName());
        } else {
            logger.debug("command.getCommandExecutorName() = " + command.getCommandExecutorName());
            executor = executorTable.get(command.getCommandExecutorName());

            if (executor == null) {
                throw new CommandManagerException("CommandExecutor \"" + command.getCommandExecutorName() + "\" not found!");
            }
        }

        logger.debug("the \"" + executor.getName() + "\" executor selected for the execution of the command \"" + command.getName() + "\"");
    
        if (command.isInternal()) {
            logger.debug("new command [" + command.toString() + "]");
        } else {
            logger.info("new command [" + command.toString() + "]");
        }
        
        if (!executor.checkCommandSupport(command.getName())) {
            command.setFailureReason("command not supported by the \"" + executor.getName() + "\" executor!");
            command.setStatus(Command.ERROR);

            logger.info("new command [" + command.toString() + "]");

            throw new CommandException("command \"" + command.getName() + "\" not supported by the \"" + executor.getName() + "\" executor!");
        }
 
        if (command.isAsynchronous()) {
            try {
                executor.getCommandQueue().enqueue(command);

                command.setStatus(Command.QUEUED);

                if (logger.isDebugEnabled()) {
                    logger.debug("status change for command [" + command.toString() + "]");
                }
            } catch (Throwable e) {
                logger.error("cannot enqueue the command " + command.getName() + ": " + e.getMessage());

                command.setFailureReason(e.getMessage());
                command.setStatus(Command.ERROR);

                logger.info("status change for command [" + command.toString() + "]");
 
                throw new CommandException("cannot enqueue the command " + command.getName() + ": " + e.getMessage());
            }
        } else {            
            try {
                command.setId(Calendar.getInstance().getTimeInMillis());
                command.setStatus(Command.SCHEDULED);

                if (logger.isDebugEnabled()) {
                    logger.debug("status change for command [" + command.toString() + "]");
                }

                command.setStatus(Command.EXECUTING);

                if (logger.isDebugEnabled()) {
                    logger.debug("status change for command [" + command.toString() + "]");
                }

                executor.execute(command);
                
                command.setStatus(Command.EXECUTED_OK);

                if (logger.isDebugEnabled()) {
                    logger.debug("status change for command [" + command.toString() + "]");
                }
            } catch (Exception e) {
                logger.error("CommandManager execute: " + command.getName() + " error " + e.getMessage());

                command.setFailureReason(e.getMessage());
                command.setStatus(Command.EXECUTED_ERROR);

                logger.info("status change for command [" + command.toString() + "]");

                throw new CommandException(e.getMessage());
            }
        }
        
        logger.debug("END execute");
    }

    public void execute(List<Command> commandList) throws CommandException, CommandManagerException {
        if (commandList == null) {
            throw new CommandException("Command list not specified!");
        }

        for (Command command : commandList) {
            execute(command);
        }
    }

    public List<String> getCommandCategory() {
        List<String> categoryList = new ArrayList<String>(0);
        if (!commandExecutorCategory.isEmpty()) {
            categoryList.addAll(commandExecutorCategory.keySet());
        }

        return categoryList;
    }

    public CommandExecutorInterface getCommandExecutor(String name, String category) throws CommandManagerException {
        if (name == null) {
            throw new CommandManagerException("name not specified!");
        }

        if (category != null) {
            if (commandExecutorCategory.containsKey(category)) {
                Hashtable<String, CommandExecutorInterface> commandExecutortable = commandExecutorCategory.get(category);
                if (commandExecutortable.containsKey(name)) {
                    return commandExecutortable.get(name);
                }

                throw new CommandManagerException("CommandExecutor \"" + name + "\" not found!");
            }

            throw new CommandManagerException("Category \"" + category + "\" not found!");
        } else {
            for (Hashtable<String, CommandExecutorInterface> commandExecutortable : commandExecutorCategory.values()) {
                if (commandExecutortable.containsKey(name)) {
                    return commandExecutortable.get(name);
                }
            }
            throw new CommandManagerException("CommandExecutor \"" + name + "\" not found!");
        }
    }

    public List<CommandExecutorInterface> getCommandExecutors() {
        List<CommandExecutorInterface> commandExecutorList = new ArrayList<CommandExecutorInterface>(0);
        if (!commandExecutorCategory.isEmpty()) {
            for (Hashtable<String, CommandExecutorInterface> commandExecutortable : commandExecutorCategory.values()) {
                commandExecutorList.addAll(commandExecutortable.values());
            }
        }

        return commandExecutorList;
    }

    public long getCurrentThroughput() {
        return currentThroughput.get();
    }

    public Calendar getLastThroughputUpdate() {
        return lastThroughputUpdate;
    }

    public long getMaxThroughput() {
        return maxThroughput.get();
    }

    public void init() throws CommandManagerException {
        commandExecutorCategory = new Hashtable<String, Hashtable<String, CommandExecutorInterface>>(0);
        lastThroughputUpdate = Calendar.getInstance();
    

        if (initialization == INITIALIZATION_OK) {
            return;
        }
        
        ServiceConfig service = ServiceConfig.getConfiguration();
        if (service == null) {
            initialization = INITIALIZATION_ERROR;
            throw new CommandManagerException("Cannot get instance of the Configuration Service");
        }

        List<CommandExecutorConfig> commandExecutorList = service.getCommandExecutorList();
        if (commandExecutorList == null || commandExecutorList.size() == 0) {
            initialization = INITIALIZATION_OK;
            return;
        }

        try {
            for (CommandExecutorConfig commandExecutorConfig : commandExecutorList) {
                addCommandExecutor(commandExecutorConfig);
            }           
        } catch (CommandManagerException e) {
            initialization = INITIALIZATION_ERROR;
            terminate();
            logger.error("initialization error: " + e.getMessage());
            throw e;
        }

        initialization = INITIALIZATION_OK;
    }

    public void removeCommandExecutor(CommandExecutorInterface commandExecutor) throws CommandManagerException {
        if (commandExecutor == null) {
            throw new CommandManagerException("CommandExecutor not specified!");
        }

        removeCommandExecutor(commandExecutor.getName(), commandExecutor.getCategory());
    }

    public void removeCommandExecutor(String name, String category) throws CommandManagerException {
        if (category == null) {
            throw new CommandManagerException("CommandExecutor's category not specified!");
        }

        if (name == null) {
            throw new CommandManagerException("CommandExecutor's name not specified!");
        }

        if (commandExecutorCategory.containsKey(category)) {
            Hashtable<String, CommandExecutorInterface> executorTable = commandExecutorCategory.get(category);

            CommandExecutorInterface commandExecutor = executorTable.remove(name);
            if (commandExecutor != null) {
                try {
                    commandExecutor.destroy();
                } catch (CommandExecutorException e) {
                    logger.error("failure on destroying the CommandExecutor \"" + name + "\"");
                    throw new CommandManagerException("failure on destroying the CommandExecutor \"" + name + "\"");
                }
                logger.debug("CommandExecutorInterface \"" + name + "\" removed");
            } else {
                throw new CommandManagerException("CommandExecutor \"" + name + "\" not found!");
            }
        } else {
            throw new CommandManagerException("Category \"" + category + "\" not found!");
        }
    }

    public void run() {
        long maxThroughputTmp = 0L;
        long currentThroughputTmp = 0L;
        long elapsedTimeInMillis = 0L;
        Calendar now = null;
        StringBuffer buff = null;

        while (!exit) {
            logger.debug("evaluating the throughput...");

            now = Calendar.getInstance();
            buff = new StringBuffer("throughput evaluated:\n");
            elapsedTimeInMillis = now.getTimeInMillis() - lastThroughputUpdate.getTimeInMillis();

            if (!commandExecutorCategory.isEmpty()) {
                Collection<Hashtable<String, CommandExecutorInterface>> executorCategoryList = commandExecutorCategory.values();
                Collection<CommandExecutorInterface> executorList = null;
                CommandQueueInterface queue = null;
                
                for (Hashtable<String, CommandExecutorInterface> executorCategory : executorCategoryList) {
                    executorList = executorCategory.values();

                    for (CommandExecutorInterface executor : executorList) {
                        queue = executor.getCommandQueue();

                        if (queue != null) {
                            try {
                                queue.evalutateThroughput();

                                buff.append("[queue=" + queue.getName() + "; currentInThroughput=" + queue.getCurrentInThroughput() +
                                        " cmd/min; maxInThroughput=" + queue.getMaxInThroughput() + " cmd/min; currentOutThroughput=" + queue.getCurrentOutThroughput() +
                                        " cmd/min; maxOutThroughput=" + queue.getMaxOutThroughput() + " cmd/min]\n");
                            } catch (Throwable t) {
                               logger.error("failure on evalutating the throughput of the " + executor.getName() + "'s queue: " + t.getMessage());
                            }
                        }
                    }
                }
            }
   
            synchronized (lastThroughputUpdate) {
                lastThroughputUpdate.setTime(now.getTime());

                maxThroughputTmp = maxThroughput.get();

                currentThroughputTmp = 0;
                if (commandsCount.get() > 0) {
                    currentThroughputTmp = (commandsCount.get() * 60000 / elapsedTimeInMillis);
                }

                currentThroughput.set(currentThroughputTmp);

                if (maxThroughputTmp < currentThroughputTmp) {
                    maxThroughputTmp = currentThroughputTmp;
                    maxThroughput.set(maxThroughputTmp);
                }

                commandsCount.set(0);
            }

            buff.append("[CommandManager currentThroughput=" + currentThroughputTmp + " cmd/min; maxThroughput=" + maxThroughputTmp + " cmd/min]");

            logger.info(buff.toString());
            logger.debug("evaluating the throughput... done!");

            synchronized (lock) {
                try {
                    lock.wait(EVALUATION_RATE);
                } catch (Throwable e) {
                    logger.error(e.getMessage());
                }   
            }
        }
    }

    public void terminate() {
        logger.info("terminate invoked!");
        exit = true;

        if (!commandExecutorCategory.isEmpty()) {
            Collection<Hashtable<String, CommandExecutorInterface>> executorCategoryList = commandExecutorCategory.values();
            Collection<CommandExecutorInterface> executorList = null;

            for (Hashtable<String, CommandExecutorInterface> executorCategory : executorCategoryList) {
                executorList = executorCategory.values();

                for (CommandExecutorInterface executor : executorList) {
                    try {
                        logger.info("terminating the " + executor.getName() + " executor");
                        executor.destroy();
                        logger.info(executor.getName() + " executor termitated!");
                    } catch (Throwable t) {
                        logger.error("failure on terminating the CommandExecutor \": " + executor.getName() + "\": " + t.getMessage());
                    }
                }
            }
        }

        commandExecutorCategory.clear();

        synchronized (lock) {
            lock.notifyAll();
        }

        commandManager = null;
        logger.info("terminated!");
    }
}

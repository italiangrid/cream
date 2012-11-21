package org.glite.ce.cream.activitymanagement.db;

import java.util.Hashtable;
import java.util.List;
import java.util.Random;

import javax.xml.datatype.XMLGregorianCalendar;

import org.glite.ce.commonj.db.DatabaseException;
import org.glite.ce.creamapi.activitymanagement.Activity;
import org.glite.ce.creamapi.activitymanagement.ActivityCommand;
import org.glite.ce.creamapi.activitymanagement.ActivityStatus;
import org.glite.ce.creamapi.activitymanagement.ActivityStatus.StatusName;
import org.glite.ce.creamapi.activitymanagement.ListActivitiesResult;
import org.glite.ce.creamapi.activitymanagement.db.ActivityDBInterface;

public class ActivityDBInMemory implements ActivityDBInterface {
    private static final Random activityIdGenerator = new Random();
    private Hashtable<String, Hashtable<String, Activity>> activityDB = null;
    private static ActivityDBInMemory activityDBManager = null;

    public static ActivityDBInterface getInstance() {
        if (activityDBManager == null) {
            activityDBManager = new ActivityDBInMemory();
        }

        return activityDBManager;
    }

    private ActivityDBInMemory() {
        activityDB = new Hashtable<String, Hashtable<String, Activity>>(0);
    }

    public void deleteActivity(String activityId, String userId) throws DatabaseException, IllegalArgumentException {
        if (activityId == null) {
            throw new IllegalArgumentException("activityId not specified!");
        }

        if (userId == null) {
            throw new IllegalArgumentException("userId not specified!");
        }

        Hashtable<String, Activity> userDB = activityDB.get(userId);
        
        if (userDB != null) {
            userDB.remove(activityId);
        }
    }

    public Activity getActivity(String activityId, String userId) throws DatabaseException, IllegalArgumentException {
        if (activityId == null) {
            throw new IllegalArgumentException("activityId not specified!");
        }

        if (userId == null) {
            for (Hashtable<String, Activity> userDB : activityDB.values()) {
                if (userDB.containsKey(activityId)) {
                    return userDB.get(activityId);
                }
            }
        } else {
            Hashtable<String, Activity> userDB = activityDB.get(userId);

            if (userDB != null) {
                return userDB.get(activityId);
            }
        }
        return null;
    }

    public String insertActivity(Activity activity) throws DatabaseException, IllegalArgumentException {
        if (activity == null) {
            throw new IllegalArgumentException("Activity not specified!");
        }

        if (activity.getUserId() == null) {
            throw new IllegalArgumentException("userId not specified!");
        }

        String activityId = "000000000";

        synchronized (activityIdGenerator) {
            activityId += activityIdGenerator.nextInt(1000000000);
            activityId = activityId.substring(activityId.length() - 9);
            activityId = "CR_ES" + activityId;
        }

        activity.setId(activityId);

        Hashtable<String, Activity> userDB = activityDB.get(activity.getUserId());
        
        if (userDB == null) {
            userDB = new Hashtable<String, Activity>(0);
            activityDB.put(activity.getUserId(), userDB);
        }
        
        if (userDB.containsKey(activityId)) {
            throw new DatabaseException("Activity " + activityId + " already exists!");
        }

        userDB.put(activityId, activity);
        return activityId;
    }

    public void insertActivityCommand(String activityId, ActivityCommand activityCommand) throws DatabaseException, IllegalArgumentException {
        if (activityCommand == null) {
            throw new IllegalArgumentException("ActivityCommand not specified!");
        }
        
        Activity activity = getActivity(activityId, null);
        
        if (activity == null) {
            throw new DatabaseException("activity not found");
        }
        
        activity.getCommands().add(activityCommand);
    }

    public void insertActivityStatus(String activityId, ActivityStatus activityStatus) throws DatabaseException, IllegalArgumentException {
        if (activityStatus == null) {
            throw new IllegalArgumentException("ActivityStatus not specified!");
        }

        Activity activity = getActivity(activityId, null);
        
        if (activity == null) {
            throw new DatabaseException("activity not found");
        }
        
        activity.getStates().add(activityStatus);
    }

    public ListActivitiesResult listActivities(XMLGregorianCalendar fromDate, XMLGregorianCalendar toDate, List<ActivityStatus> statusList, int limit, String userId) throws DatabaseException, IllegalArgumentException {
        if (userId == null) {
            throw new IllegalArgumentException("userId not specified!");
        }

        ListActivitiesResult result = new ListActivitiesResult();
        result.setIsTruncated(Boolean.FALSE);
        
        Hashtable<String, Activity> userDB = activityDB.get(userId);
        
        if (userDB != null) {
            result.getActivityIdList().addAll(userDB.keySet());
        }
        
        return result;
    }

    public void updateActivity(Activity activity) throws DatabaseException, IllegalArgumentException {
        if (activity == null) {
            throw new IllegalArgumentException("Activity not specified!");
        }
        
        if (activity.getId() == null) {
            throw new IllegalArgumentException("activityId not specified!");
        }

        if (activity.getUserId() == null) {
            throw new IllegalArgumentException("userId not specified!");
        }
        
        Hashtable<String, Activity> userDB = activityDB.get(activity.getUserId());

        if (userDB == null || !userDB.containsKey(activity.getId())) {
            throw new DatabaseException("Activity " + activity.getId() + " not found!");
        }
        
        userDB.put(activity.getId(), activity);
    }

    public void updateActivityCommand(ActivityCommand activityCommand) throws DatabaseException, IllegalArgumentException {
        // TODO Auto-generated method stub
        
    }

    public void updateActivityStatus(ActivityStatus activityStatus) throws DatabaseException, IllegalArgumentException {
        // TODO Auto-generated method stub
        
    }

    public String retrieveOlderActivityId(List<StatusName> statusList, String userId) throws DatabaseException, IllegalArgumentException {
        // TODO Auto-generated method stub
        return null;
    }

    public List<String> listActivitiesForStatus(List<StatusName> statusList, String userId, int dateValue) throws DatabaseException, IllegalArgumentException {
        // TODO Auto-generated method stub
        return null;
    }
}
###################################################################
#                            Signals
###################################################################
 
trap 'fatal_error "Job has been terminated (got SIGXCPU)" "OSB" "CANCEL"' XCPU
trap 'fatal_error "Job has been terminated (got SIGQUIT)" "OSB" "CANCEL"' QUIT
trap 'fatal_error "Job has been terminated (got SIGINT)" "OSB" "CANCEL"' INT
trap 'fatal_error "Job has been terminated (got SIGTERM)" "OSB" "CANCEL"' TERM
###################################################################
#                           Environment
###################################################################

jw_host="`hostname -f`"

for((idx=0; idx<${#__environment[*]}; idx++)); do
  eval export ${__environment[$idx]}
done

if [ -z "${GLITE_LOCAL_COPY_RETRY_COUNT_ISB}" ]; then
  if [ -n "${GLITE_LOCAL_COPY_RETRY_COUNT}" ]; then
    __copy_retry_count_isb=${GLITE_LOCAL_COPY_RETRY_COUNT}
  fi
else
  __copy_retry_count_isb=${GLITE_LOCAL_COPY_RETRY_COUNT_ISB}
fi

if [ -z "${GLITE_LOCAL_COPY_RETRY_COUNT_OSB}" ]; then
  if [ -n "${GLITE_LOCAL_COPY_RETRY_COUNT}" ]; then
    __copy_retry_count_osb=${GLITE_LOCAL_COPY_RETRY_COUNT}
  fi
else
  __copy_retry_count_osb=${GLITE_LOCAL_COPY_RETRY_COUNT_OSB}
fi
  
if [ -z "${GLITE_LOCAL_COPY_RETRY_FIRST_WAIT_ISB}" ]; then
  if [ -n "${GLITE_LOCAL_COPY_RETRY_FIRST_WAIT}" ]; then
    __copy_retry_first_wait_isb=${GLITE_LOCAL_COPY_RETRY_FIRST_WAIT}
  fi
else
  __copy_retry_first_wait_isb=${GLITE_LOCAL_COPY_RETRY_FIRST_WAIT_ISB}
fi

if [ -z "${GLITE_LOCAL_COPY_RETRY_FIRST_WAIT_OSB}" ]; then
  if [ -n "${GLITE_LOCAL_COPY_RETRY_FIRST_WAIT}" ]; then
    __copy_retry_first_wait_osb=${GLITE_LOCAL_COPY_RETRY_FIRST_WAIT}
  fi
else
  __copy_retry_first_wait_osb=${GLITE_LOCAL_COPY_RETRY_FIRST_WAIT_OSB}
fi

###################################################################
#                           functions definitions
###################################################################

do_transfer() # 1 - command, 2 - source, 3 - dest, 4 - std err, 5 - exit code file
{
  eval "$1" "$2" "$3" 2>"$4"
  echo $? > "$5"
}
###################################################################

doExit() # 1 - status, 2 - reason 
{
  aw_status=$1
  echo "aw exit status = ${aw_status}"
  echo $2 1>&2

  if [ $__create_subdir -eq 1 ]; then
    cd ..
    rm -rf ${newdir}
  fi

  # customization point #3
  if [ -n "${GLITE_LOCAL_CUSTOMIZATION_DIR}" ]; then
    if [ -r "${GLITE_LOCAL_CUSTOMIZATION_DIR}/cp_3.sh" ]; then
      . "${GLITE_LOCAL_CUSTOMIZATION_DIR}/cp_3.sh"
    fi
  fi
  
  exit ${aw_status}
}
###################################################################

retry_copy() # 1 - command, 2 - source, 3 - dest, 4 - proxyFileNamePath 
{
  count=0
  succeded=1
  sleep_time=0
  source="$2"
  dest="$3"
  proxyFileNamePath="$4"
  
  if [ "$1" == "\${globus_transfer_cmd}" ]; then
      command="$1 -cred $4"
  elif [ "$1" == "\${https_transfer_cmd}" ]; then
       command="$1 --cert $4"
  fi
  
  local match_index=`expr match "${source}" '[[:alpha:]][[:alnum:]+.-]*://'`
  if [ ${match_index} -gt 0 ]; then
    match_index=`expr ${match_index} - 3`
  fi
  local scheme_src=${source:0:${match_index}}

  local match_index=`expr match "${dest}" '[[:alpha:]][[:alnum:]+.-]*://'`
  if [ ${match_index} -gt 0 ]; then
    match_index=`expr ${match_index} - 3`
  fi
  local scheme_dest=${dest:0:${match_index}}
  
   if [ "x${scheme_src}" == "xfile" -o "x${scheme_src}" == "x" ]; then
    __copy_retry_count=${__copy_retry_count_osb}
    __copy_retry_first_wait=${__copy_retry_first_wait_osb}
  elif [ "x${scheme_dest}" == "xfile" -o "x${scheme_dest}" == "x" ]; then
    __copy_retry_count=${__copy_retry_count_isb}
    __copy_retry_first_wait=${__copy_retry_first_wait_isb}
  fi    
  
  while [ $count -le ${__copy_retry_count} -a $succeded -ne 0 ];
  do
    time_left=`grid-proxy-info -file $proxyFileNamePath -timeleft 2>/dev/null || echo -100`;
    # -100 if there's some problem to detect the lifetime of the proxy $proxyFileNamePath.

    if [ $time_left -eq -100 ]; then
      retry_error_message="Problem to detect the lifetime of the proxy $proxyFileNamePath"
      return 1
    fi

    if [ $time_left -lt $sleep_time ]; then
      retry_error_message="proxy expired: $proxyFileNamePath"
      return 1
    fi
    sleep $sleep_time
    if [ $sleep_time -eq 0 ]; then
      sleep_time=${__copy_retry_first_wait}
    else
      sleep_time=`expr $sleep_time \* 2`
    fi
    transfer_stderr=`mktemp -q std_err.XXXXXXXXXX`
    if [ ! -f "$transfer_stderr" ]; then
      transfer_stderr="/dev/null"
    fi
    transfer_exitcode=`mktemp -q tr_exit_code.XXXXXXXXXX`
    if [ ! -f "$transfer_exitcode" ]; then
      transfer_exitcode="/dev/null"
    fi
    do_transfer "$command" "${source}" "${dest}" "$transfer_stderr" "$transfer_exitcode"&
    transfer_watchdog_pid=$!
    transfer_timeout=3600
    while [ $transfer_timeout -gt 0 ];
    do
      if [ -z `ps -p $transfer_watchdog_pid -o pid=` ]; then
        break;
      fi
      sleep 1
      let "transfer_timeout--"
    done
    if [ $transfer_timeout -le 0 ]; then
      kill -9 $transfer_watchdog_pid
      retry_error_message="Hanging transfer"
      succeded=1
    else
      succeded=`cat $transfer_exitcode 2>/dev/null`
      if [ -z $succeded ]; then
        retry_error_message="Cannot retrieve return value for transfer"
        succeded=1
      else
        retry_error_message=`cat $transfer_stderr 2>/dev/null`
      fi
    fi
    rm -f "$transfer_stderr" "$transfer_exitcode"
    count=`expr $count + 1`
  done
  return ${succeded}
}
###################################################################

perl_notifier='
use Socket;

sub send_notify {
    $es_url = "'${__logger_dest}'";die "No es url" unless $es_url;
    ($remote, $port) = split /:/, $es_url, 2;die "Missing port" unless $port;

    ($prefix, $activity_id) = split /CR_ES/,"'${__esActivityId}'" , 2;die "Missing activity id" unless $activity_id;

    $event_infos = $_[0];die "No infos" unless $event_infos;

    $iaddr   = inet_aton($remote) or die "no host: $remote";
    $paddr   = sockaddr_in($port, $iaddr);
    $proto   = getprotobyname("tcp");

    ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime;
    $change_time = sprintf "%4d-%02d-%02d %02d:%02d:%02d" , $year+1900 , $mon+1, $mday, $hour, $min, $sec;

    $hostname = `hostname`;
    chomp $hostname;

    socket(SOCK, PF_INET, SOCK_STREAM, $proto) or die "socket: $!";
    connect(SOCK, $paddr) or die "connect: $!";
    print SOCK "[${event_infos} ClientJobId=\"${activity_id}\"; ChangeTime=\"${change_time}\"; WorkerNode=\"${hostname}\";]\n";

    close (SOCK) or die "close: $!";
}'
###################################################################

function notifyToES {
  if [ -n "${__logger_dest}" ]; then
    perl -e "${perl_notifier}"' send_notify(@ARGV);' "$1"
    return $?
  fi
  return 0
}
###################################################################

jw_echo() # 1 - msg
{
  echo "$1"
}
###################################################################

fatal_error() # 1 - reason, 2 - transfer OSB, 3 - status
{
  notifyToES "JobStatus=4; Reason=\"$1\";"
  #log_done_failed 1 "$1"
  if [ "x$2" == "xOSB" ]; then
    OSB_transfer $3
  fi
  doExit 1 "$1"
}
###################################################################

truncate() # 1 - file name, 2 - bytes num., 3 - name of the truncated file
{
  tail "$1" --bytes=$2>$3 2>/dev/null
  return $?
}
###################################################################

OSB_transfer() # 1 - status
{ 
declare -a __all_output_file
declare -a __output_cmd_line_command
declare -a __output_cmd_line_source
declare -a __output_cmd_line_dest
declare -a __output_cmd_line_proxy
declare -a __output_cmd_line_file_flag

if [ "$1" == "CANCEL" ]; then
   flag_index=1
elif [ "$1" == "FAILURE" ]; then
   flag_index=2
elif [ "$1" == "SUCCESS" ]; then
   flag_index=3
fi
  
if [ -n "${__output_file[0]}" ]; then
  item_counter=0
  file_counter=0
 
  for output_item in `eval echo ${__output_file[@]}`; do
    missing_item=1
      if [ -f ${output_item} ]; then
        missing_item=0
        __all_output_file[$file_counter]="$output_item"
        __output_cmd_line_command[$file_counter]="${__output_transfer_cmd[${item_counter}]}"
        __output_cmd_line_source[$file_counter]="file://${output_item}"
        __output_cmd_line_dest[$file_counter]="${__output_file_dest[${item_counter}]}"
        __output_cmd_line_proxy[$file_counter]="${__output_proxy_file[${item_counter}]}"
        __output_cmd_line_file_flag[$file_counter]="${__output_file_flag[${item_counter}]}"
        file_counter=$[${file_counter}+1]
      fi
      
      if [ $missing_item -eq 1 ] && [ ${__output_file_flag[${item_counter}]::1} -eq 1 ] && [ ${__output_file_flag[${item_counter}]:$flag_index:1} -eq 1 ]; then
        fatal_error "Cannot read or missing output file mandatory ${output_item} for status $1"
      fi

      if [ $missing_item -eq 1 ] && [ ${__output_file_flag[${item_counter}]:$flag_index:1} -eq 1 ]; then
        echo "Cannot read or missing output file ${output_item} for status $1"
      fi
      item_counter=$[${item_counter}+1]
  done
fi

  output_file_mandatory_ok=0;
  for ((idx=0; idx<${#__all_output_file[*]}; idx++)); do
    if [ ${__output_cmd_line_file_flag[$idx]::1} -eq 1 ] && [ ${__output_cmd_line_file_flag[$idx]:$flag_index:1} -eq 1 ]; then 
       retry_copy "${__output_cmd_line_command[$idx]}" "${__output_cmd_line_source[$idx]}" "${__output_cmd_line_dest[$idx]}" "${__output_cmd_line_proxy[$idx]}"
       if [ $? != 0 ]; then
          fatal_error "Fatal Error: cannot upload ${__output_cmd_line_source[$idx]} into ${__output_cmd_line_dest[$idx]} with proxy ${__output_cmd_line_proxy[$idx]}"
       fi
       output_file_mandatory_ok=1;
    fi
  done
  
  #if no output file mandatory
  if [ $output_file_mandatory_ok -eq 0 ]; then
    for ((idx=0; idx<${#__all_output_file[*]}; idx++)); do
      if [ ${__output_cmd_line_file_flag[$idx]::1} -eq 0 ]; then 
         retry_copy "${__output_cmd_line_command[$idx]}" "${__output_cmd_line_source[$idx]}" "${__output_cmd_line_dest[$idx]}" "${__output_cmd_line_proxy[$idx]}"
         if [ $? != 0 ]; then
            echo "Cannot upload ${__output_cmd_line_source[$idx]} into ${__output_cmd_line_dest[$idx]} with proxy ${__output_cmd_line_proxy[$idx]}"
         fi
      fi
    done
  fi 
     
  #LRMS OSB
  for((idx=0; idx<${#__lrms_output_file[*]}; idx++)); do
    `mv ${workdir}/${__lrms_output_file[$idx]} ${workdir}/../` 
    if [ $? != 0 ]; then
      warning_message="Cannot move ${workdir}/${__lrms_output_file[$idx]} to ${workdir}/../"
      #log_user_event "$warning_message"
      echo "$warning_message"
    fi
  done
}

###################################################################
#                           let's start it up
###################################################################

# customization point #1
# Be sure to update workdir as it may be changed by cp_1.sh
if [ -n "${GLITE_LOCAL_CUSTOMIZATION_DIR}" ]; then
  if [ -r "${GLITE_LOCAL_CUSTOMIZATION_DIR}/cp_1.sh" ]; then
    . "${GLITE_LOCAL_CUSTOMIZATION_DIR}/cp_1.sh"
  fi
fi

notifyToES "JobStatus=2;"
#log_event "Running"

#################### BEGIN VO-HOOK #################################
vo_hook="lcg-jobwrapper-hook.sh" # common-agreed now hard-coded

if [ -z "${__ce_application_dir}" -a -n "${__vo}" ]; then
     tmp=`echo "${__vo}" | sed 's/[-.]/_/g' | tr '[a-z]' '[A-Z]'`
     eval __ce_application_dir=\$VO_${tmp}_SW_DIR
fi
if [ -n "${__ce_application_dir}" ]; then
  if [ -d "${__ce_application_dir}" ]; then
    if [ -r "${__ce_application_dir}/${vo_hook}" ]; then
      . "${__ce_application_dir}/${vo_hook}"
    elif [ -r "${__ce_application_dir}/${__vo}/${vo_hook}" ]; then
      . "${__ce_application_dir}/${__vo}/${vo_hook}"
    else
      jw_echo "${vo_hook} not readable or not present"
    fi
  else
    jw_echo "${__ce_application_dir} not found or not a directory"
  fi
fi
unset vo_hook
#################### END VO-HOOK ###################################

if [ $__create_subdir -eq 1 ]; then
  newdir="${__working_directory}"
  mkdir ${newdir}
  cd ${newdir}
fi

touch .tmp_file
if [ $? -ne 0 ]; then
  fatal_error "Working directory not writable"
fi

globus_transfer_cmd=globus-url-copy
https_transfer_cmd=`which htcp 2>/dev/null`
if [ $? != 0 ]; then
  https_transfer_cmd=globus-url-copy
fi

workdir="`pwd`"

# Check if grid-proxy-info is in the path
`which grid-proxy-info 1>/dev/null 2>/dev/null`
if [ $? != 0 ]; then
  fatal_error "Cannot find grid-proxy-info"
fi

###################################################################
#                             ISB
###################################################################

#LRMS ISB
for((idx=0; idx<${#__lrms_input_file[*]}; idx++)); do
  `mv ${workdir}/../${__lrms_input_file[$idx]} ${workdir}` 
  if [ $? != 0 ]; then
    fatal_error "Cannot move ${workdir}/../${__lrms_input_file[$idx]} to ${workdir}"
    elif [ ${__lrms_input_file_isExecutable[$idx]} -eq 1 ]; then
     chmod +x "${workdir}/${__lrms_input_file[$idx]}" 2>/dev/null
  fi
done

input_file_prec=""
input_file_prec_result=0
retry_file_result=""

for((idx=0; idx<${#__input_transfer_cmd[*]}; idx++)); do
if [ "X$input_file_prec" != "X${__input_file_dest[$idx]}" ] && [ $input_file_prec_result -ne 0 ]; then
    fatal_error "$retry_file_result Cannot move ISB (retry_copy ${__input_transfer_cmd[$idx]} ${__input_file_url[$idx]} file://${workdir}/${__input_file_dest[$idx]}): ${retry_error_message}"
elif [ "X$input_file_prec" != "X${__input_file_dest[$idx]}" -a $input_file_prec_result -eq 0 ] || [ $input_file_prec_result -ne 0 ]; then
  retry_copy "${__input_transfer_cmd[$idx]}" "${__input_file_url[$idx]}" "file://${workdir}/${__input_file_dest[$idx]}" "${__input_proxy_file[$idx]}"
  input_file_prec_result=$?
  if [ $input_file_prec_result -ne 0 ]; then
    retry_file_result="$retry_file_result Cannot move ISB (retry_copy ${__input_transfer_cmd[$idx]} ${__input_file_url[$idx]} file://${workdir}/${__input_file_dest[$idx]}): ${retry_error_message}"
  else 
    retry_file_result=""
  fi
  input_file_prec="${__input_file_dest[$idx]}"
  if [ $input_file_prec_result -eq 0 ] &&  [ ${__input_file_isExecutable[$idx]} -eq 1 ]; then
     chmod +x "${workdir}/${__input_file_dest[$idx]}" 2>/dev/null
  fi
fi
done

if [ $input_file_prec_result -ne 0 ]; then
  fatal_error "$retry_file_result Cannot move ISB (retry_copy ${__input_transfer_cmd[$idx]} ${__input_file_url[$idx]} file://${workdir}/${__input_file_dest[$idx]}): ${retry_error_message}"
fi

###################################################################
#                             JOB
###################################################################

if [ -e "${__executable}" ]; then
   chmod +x "${__executable}" 2> /dev/null
else
   fatal_error "${__executable} not found or unreadable"
fi

#################### BEGIN PREEXECUTABLE #################################
for((idx=0; idx<${#__preExecutable_path[*]}; idx++)); do
  if [ ! -r "${__preExecutable_path[$idx]}" ]; then
    fatal_error "PreExecutable ${__preExecutable_path[$idx]} not found"
  fi
  chmod +x "${__preExecutable_path[$idx]}" 2>/dev/null
  eval ${__preExecutable_path[$idx]} "${__preExecutable_arguments[$idx]}"
  preExecutable_status=$?
  
  if [ "${__preExecutable_exitCode[$idx]}" != "XXX" -a "${preExecutable_status}" != "${__preExecutable_exitCode[$idx]}" ] ; then
    fatal_error "PreExecutable ${__preExecutable_path[$idx]} failed with error ${preExecutable_status} instead of ${__preExecutable_exitCode[$idx]}"
  fi
done
#################### END PREEXECUTABLE #################################

notifyToES "JobStatus=11;"
#log_event "ReallyRunning"

perl -e "${perl_notifier}"'

use threads;

sub get_exit_value {
  my ($result) = @_;

  my ($exit_value,$signal_num,$dumped_core)  = ( $result >> 8, $result & 0x7F, ($result & 0x80) ? 1 : 0 );
  return $exit_value;
}

sub max {
    if ($_[0]<$_[1]) {return $_[1]} else {return $_[0]};
} 

sub proxy_renewal {
    my ($delegationProxyCESandboxPath, $delegationProxyLocalSandboxPathTmp, $delegationProxyLocalFile) = @_;
    $dtime_slot = $ENV{__delegationTimeSlot};
    $time_left_error = 0;
    $time_left = 0; 

    if ($dtime_slot != -1) {
        while (1) {
            $grid_proxy_info_output = `grid-proxy-info -file $delegationProxyLocalFile 2>/dev/null` || -100;
            if ($grid_proxy_info_output == -100) {
                $time_left_error = -1;
                $time_left = -1;
            }

            last if ($time_left == -1);

            if ($grid_proxy_info_output =~ /timeleft\s+:\s+(\d+):(\d+):(\d+)/) {
               ($hr,$min,$sec) = ($1,$2,$3);
               $time_left = $hr*3600 + $min*60 + $sec;
               #$currenttime=`date`;
            } else {
                $time_left_error = -1;
                $time_left = 0;
            }

            last if ($time_left <= 0);

            $succeed = 1;
            if ($time_left <= $dtime_slot) {
                $currenttime = localtime time;
                print "$currenttime: Downloading proxy $delegationProxyCESandboxPath from CE node ...\n";
                $succeed = get_exit_value(system("globus-url-copy -cred ". $delegationProxyLocalFile . " -dcpriv " . $delegationProxyCESandboxPath . " " . "file://". $delegationProxyLocalSandboxPathTmp ." >/dev/null 2>&1"));

                if ($succeed == 0) {
                    $succeed = get_exit_value(system("chmod 600". " " . $delegationProxyLocalSandboxPathTmp ." >/dev/null 2>&1"));
                } else {
                    $currenttime = localtime time;
                    print STDERR "$currenttime: Failure in downloading proxy $delegationProxyCESandboxPath from CE node\n";
                    print "$currenttime: Failure in downloading proxy $delegationProxyCESandboxPath from CE node\n";
                }

                if ($succeed == 0) {
                    $succeed = get_exit_value(system("mv". " " . $delegationProxyLocalSandboxPathTmp . " " . $delegationProxyLocalFile . " >/dev/null 2>&1"));
                    $currenttime = localtime time;
                    print "$currenttime: Done: proxy $delegationProxyCESandboxPath downloaded from CE node\n";
                }
                sleep(max($ENV{__copy_proxy_min_retry_wait}, int((0.2 + 0.1*rand()) * $time_left)));
            } else {
                $time_left = $time_left - ($dtime_slot * (1 - 0.2*rand()));
                sleep($time_left);
            }
        } #while
    } else { 
        #no renewal
        $grid_proxy_info_output = `grid-proxy-info -file $delegationProxyLocalFile 2>/dev/null` || 0;

        if ($grid_proxy_info_output =~ /timeleft\s+:\s+(\d+):(\d+):(\d+)/) {
            ($hr,$min,$sec) = ($1,$2,$3);
            $time_left = $hr*3600 + $min*60 + $sec;
            sleep($time_left);
        } else {
            $time_left_error = -1;
            $time_left = 0;
        }
    } #($dtime_slot != -1)
    
    if ( length("'${__logger_dest}'")>0 ) {
        if ($time_left_error == -1){
             send_notify("JobStatus=4;Reason=\"Problem to detect the lifetime of the proxy $delegationProxyLocalFile\";ExitCode=2;");
        } else {
             send_notify("JobStatus=4;Reason=\"Proxy $delegationProxyLocalFile is expired\";ExitCode=2;");
        }
    }

    if ($time_left_error == -1){
        print STDERR "Problem to detect the lifetime of the proxy $delegationProxyLocalFile: job killed\n";
    } else {
         print STDERR "Proxy expired ($delegationProxyLocalFile): job killed\n";
    }
    kill(9, $jobpid);
}

$executable=shift @ARGV;
#print "executable=$executable\n";
$localDir=shift @ARGV;
#print "localDir=$localDir\n";
$esActivityId=shift @ARGV;
#print "esActivityId=$esActivityId\n";
$delegationSandboxURI=shift @ARGV;
#print "delegationSandboxURI=$delegationSandboxURI\n";
$useProxyRenewal=shift @ARGV;
#print "useProxyRenewal=$useProxyRenewal\n";

@proxy=@ARGV;
    
if (!defined($jobpid = fork())) {
    die "cannot fork job: $!";

} elsif ($jobpid == 0) {    
    exec($executable);
    warn "could not exec $executable: $!\n";
    exit(127);

} elsif ($useProxyRenewal == 1 && defined($chkpid = fork()) && $chkpid == 0) {
  for (@proxy) {
      $delegationProxyCESandboxPath=$delegationSandboxURI . $_;
      $delegationProxyLocalSandboxPathTmp="/tmp/". $_ . "_" . substr($esActivityId, 5, 9);
      $delegationProxyLocalFile=$localDir . "/" . $_;
      push @thr, threads->new(\&proxy_renewal, $delegationProxyCESandboxPath, $delegationProxyLocalSandboxPathTmp, $delegationProxyLocalFile);
  }
  #join of threads
  for (@thr) {
    #print "join\n";
    $_->join();
  }
} else {
    waitpid($jobpid, 0);
    $activity_status=$?/256;
    if (defined($chkpid)) {
       kill(9, $chkpid);
    }
    print "activity exit status = $activity_status\n";
    exit($activity_status);
}' "${__cmd_line}" "${workdir}" "${__esActivityId}" "${__delegationSandboxURI}" "${__useProxyRenewal}" "${__all_proxy_file[@]}" 

exit_code=$?

if [ -n "${__executableExitCode}" -a "${__executableExitCode}" != ${exit_code} ]; then
   fatal_error "Executable exitCode ${exit_code} instead of ${__executableExitCode}" "OSB" "FAILURE"
fi

if [ -n "${GLITE_LOCAL_CUSTOMIZATION_DIR}" -a -f "${GLITE_LOCAL_CUSTOMIZATION_DIR}/cp_2.sh" ]; then
  . "${GLITE_LOCAL_CUSTOMIZATION_DIR}/cp_2.sh"
fi

#################### BEGIN POSTEXECUTABLE #################################
for((idx=0; idx<${#__postExecutable_path[*]}; idx++)); do
  if [ ! -r "${__postExecutable_path[$idx]}" ]; then
    fatal_error "PostExecutable ${__postExecutable_path[$idx]} not found" "OSB" "FAILURE"
  fi
  chmod +x "${__postExecutable_path[$idx]}" 2>/dev/null
  eval ${__postExecutable_path[$idx]} "${__postExecutable_arguments[$idx]}"
  postExecutable_status=$?
  
  if [ "${__postExecutable_exitCode[$idx]}" != "XXX" -a "${postExecutable_status}" != "${__postExecutable_exitCode[$idx]}" ] ; then
    fatal_error "PostExecutable ${__postExecutable_path[$idx]} failed with error ${postExecutable_status} instead of ${__postExecutable_exitCode[$idx]}" "OSB" "FAILURE"
  fi
done
#################### END POSTEXECUTABLE #################################

OSB_transfer "SUCCESS"

notifyToES "JobStatus=4;Reason=\"reason=0\";ExitCode=${exit_code};"

#log_done_ok "${status}" "activity completed"
doExit 0

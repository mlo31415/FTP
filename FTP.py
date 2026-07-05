from __future__ import annotations
import ftplib
from ftplib import error_perm
import json
import os
import tempfile
import io

from datetime import datetime, timedelta

from Log import Log, LogFlush, LogError
from HelpersPackage import TimestampFilename, MessageBox, Bailout


class FTP:
    g_ftp: ftplib.FTP=None      # A single FTP link for all instances of the class
    g_curdirpath: str="/"
    g_credentials: dict={}      # Saves the credentials for reconnection if the server times out
    g_dologging: bool=False      # Set FTP logging of useful debugging information by default
    _lastMessage: str=""         # Holds the last error message


    # ---------------------------------------------
    def OpenConnection(self, credentialsFilePath: str) -> bool:
        with open(credentialsFilePath) as f:
            FTP.g_credentials=json.loads(f.read())
        return self.Reconnect()     # Not exactly a reconnect, but close enough...


    #----------------------------------------------
    # Get the ID from the FTP login-in credentials
    @staticmethod
    def GetEditor() -> str:     # Deprecated: Retained for compatibility
        return FTP.UserID()
    @staticmethod
    def UserID() -> str:    # New, preferred name for method
        return FTP.g_credentials["ID"]

    # Return the last message and then clear it.
    @property
    def LastMessage(self) -> str:
        lm=FTP._lastMessage
        FTP._lastMessage=""
        return lm


    #----------------------------------------------
    # A special Log which only writes when FTP has logging turned on.
    # Used for debugging messages, b not error messages
    def Log(self, s: str, noNewLine=False) -> None:
        if FTP.g_dologging:
            Log(s, noNewLine)


    def LoggingOff(self) -> None:
        if FTP.g_dologging:
            Log("FTP Logging turned off")    # Only log a change of state
        FTP.g_dologging = False


    def LoggingOn(self) -> None:
        if not FTP.g_dologging:
            Log("FTP Logging turned on")    # Only log a change of state
        FTP.g_dologging = True

    # ---------------------------------------------
    # If we get a connection failure, reconnect tries to re-establish the connection and put the FTP object into a consistent state and then to restore the CWD
    def Reconnect(self) -> bool:
        FTP._lastMessage=""   # Clear the last essage
        self.Log("Reconnect attempted")
        if len(FTP.g_credentials) == 0:
            return False
        host=FTP.g_credentials["host"]
        try:
            FTP.g_ftp=ftplib.FTP_TLS(host=host, user=FTP.GetEditor(), passwd=FTP.g_credentials["PW"], timeout=30)
        except OSError as e:
            Log(f"***FTP.Reconnect failed to connect to {host}: {e}")
            MessageBox(f"Could not connect to the FTP server '{host}'.\n\n{e}\n\n"
                       f"The connection timed out or was refused, so no FTP work can be done.\n"
                       f"Check that you have a network connection and that '{host}' is the correct "
                       f"FTP server name in 'FTP Credentials.json' (note: a host fronted by Cloudflare "
                       f"will answer web requests but not FTP).", ignoredebugger=True, Title="FTP connection failed")
            return False
        FTP.g_ftp.prot_p()

        # Now we need to restore the current working directory
        self.Log("Reconnect: g_ftp.cwd('/')")
        msg=self.g_ftp.cwd("/")
        self.Log(msg)
        ret=msg.startswith("250 OK.")
        if not ret:
            Log("***FTP.Reconnect failed")
            return False

        self.Log("Reconnect: successful. Change directory to "+FTP.g_curdirpath)
        olddir=FTP.g_curdirpath
        FTP.g_curdirpath="/"
        self.SetDirectory(olddir)

        return True


    # ---------------------------------------------
    # Update the saved current working directory path
    # If the input is an absolute path, just use it (removing any trailing filename)
    # If it's a relative move, compute the new wd path
    def UpdateCurpath(self, newdir: str) -> None:
        newdir=newdir.replace("//", "/")
        self.Log(f"UpdateCurpath('{newdir}') ...from {FTP.g_curdirpath}")
        if newdir[0] == "/":    # Absolute directory move
            FTP.g_curdirpath=newdir
        elif newdir == "..":    # Relative move up one directory
            #TODO: Note that we don't handle things like "../.." yet
            if FTP.g_curdirpath != "/":     # If we're already at the top, we stay put.
                head, _=os.path.split(FTP.g_curdirpath)    # But we're not, so we slice off the last directory in the saved wd path
                FTP.g_curdirpath=head
        else:
            # What's left is a CD downwards
            if FTP.g_curdirpath == "/":
                FTP.g_curdirpath+=newdir
            else:
                FTP.g_curdirpath+="/"+newdir


    def GetCurPath(self) -> str:
        return self.g_curdirpath

    #---------------------------------------------
    # Given a full path "/xxx/yyy/zzz" or a single child directory thisrow (no slashes), change to that directory
    def CWD(self, newdir: str) -> bool:
        newdir=newdir.replace("//", "/")
        wd=self.PWD()
        if wd == newdir or wd+"/" == newdir:
            self.Log(f"CWD('{newdir}') from '{wd}' so already there")
            return True

        msg=""
        for attempt in range(2):
            try:
                msg=self.g_ftp.cwd(newdir)
                break
            except Exception as e:
                self.Log(f"***FTP.CWD(): attempt {attempt+1} failed. Exception={e}")
                if not self.Reconnect():
                    return False
        else:
            self.Log(f"***FTP.CWD('{newdir}'): failed after all attempts")
            return False

        self.Log(msg)
        ret=msg.startswith("250 OK.")
        if ret:
            self.UpdateCurpath(newdir)
        self.PWD()
        return ret


    # ---------------------------------------------
    # Make a new child directory named <newdir> in the current directory
    def MKD(self, newdir: str) -> bool:
        self.Log("**make directory: '"+newdir+"'")
        msg=""
        for attempt in range(2):
            try:
                msg=self.g_ftp.mkd(newdir)
                break
            except Exception as e:
                Log(f"FTP.MKD(): attempt {attempt+1} failed. Exception={e}")
                if not self.Reconnect():
                    return False
        else:
            return False
        self.Log(msg+"\n")
        return msg == newdir or msg.startswith("250 ") or msg.startswith("257 ")     # Web doc shows all three as possible.


    # ---------------------------------------------
    def DeleteFile(self, fname: str) -> bool:
        FTP._lastMessage=""   # Clear the last essage
        self.Log("**delete file: '"+fname+"'")
        if len(fname.strip()) == 0:
            Log("FTP.DeleteFile(): filename not supplied.")
            LogFlush()
            raise ValueError("Filename cannot be empty")

        if not self.FileExists(fname):
            Log("FTP.DeleteFile: '"+fname+"' does not exist.")
            return True

        msg=""
        for attempt in range(2):
            try:
                msg=self.g_ftp.delete(fname)
                break
            except Exception as e:
                Log(f"FTP.DeleteFile(): attempt {attempt+1} failed. Exception={e}")
                if not self.Reconnect():
                    return False
        else:
            return False
        self.Log(msg+"\n")
        return msg.startswith("250 ")


    # ---------------------------------------------
    def Rename(self, oldname: str, newname: str) -> bool:
        FTP._lastMessage=""   # Clear the last message
        self.Log(f"**rename file: '{oldname}'  as  '{newname}'")
        if len(oldname.strip()) == 0 or len(newname.strip()) == 0:
            Log("FTP.Rename(): oldname or newname not supplied. Probably irrecoverable, so exiting program.")
            LogFlush()
            raise ValueError("Neither oldname nor new name can be empty")

        if not self.FileExists(oldname):
            msg=f"FTP.Rename: '{oldname}' does not exist."
            FTP._lastMessage=msg
            Log(msg)
            return False

        msg=""
        for attempt in range(2):
            try:
                msg=self.g_ftp.rename(oldname, newname)
                FTP._lastMessage=msg
                break
            except Exception as e:
                Log(f"FTP.Rename: attempt {attempt+1} failed. Exception={e}")
                if not self.Reconnect():
                    return False
        else:
            return False
        self.Log(msg+"\n")
        return msg.startswith("250 ")


    # ---------------------------------------------
    # Delete a leaf-node directory and any files and empty directories it contains.
    # Note that since this does not delete recursively, the contents of any subdirectories must be deleted first.
    def DeleteDir(self, dirname: str) -> bool:
        FTP._lastMessage=""   # Clear the last message
        self.Log("**delete directory: '"+dirname+"'")
        if len(dirname.strip()) == 0:
            Log("FTP.DeleteDir(): dirname not supplied.")
            LogFlush()
            raise NotImplementedError("And they said this could never happen...")        # This should never happen.
        if dirname == "/":
            Log("FTP.DeleteDir(): Attempt to delete root -- forbidden")
            raise PermissionError("Attempt to delete root directory")

        if not self.FileExists(dirname):
            Log(f"FTP.DeleteDir(): '{dirname}' does not exist.")
            return True

        # The first step is to delete any files it contains
        files=self.Nlst(dirname)
        for file in files:
            self.DeleteFile(file)

        msg=""
        for attempt in range(2):
            try:
                msg=self.g_ftp.rmd(dirname)
                FTP._lastMessage=msg
                break
            except Exception as e:
                Log(f"FTP.DeleteDir(): attempt {attempt+1} failed. Exception={e}")
                if not self.Reconnect():
                    return False
        else:
            return False
        self.Log(msg+"\n")
        return msg.startswith("250 ")


    #----------------------------------------------
    # Compare two paths for equality.  We ignore differences in trailing "/"
    def ComparePaths(self, p1: str, p2: str) -> bool:
        p1=p1.replace("//", "/")
        p2=p2.replace("//", "/")
        # Make sure that there is a trailing "/" before comparing
        if len(p1) == 0 or p1[-1] != "/":
            p1+="/"
        if len(p2) == 0 or p2[-1] != "/":
            p2+="/"
        return p1 == p2


    # ---------------------------------------------
    # Returns the full path to the current directory as a string
    def PWD(self) -> str:
        dir=""
        for attempt in range(2):
            try:
                dir=self.g_ftp.pwd()
                break
            except Exception as e:
                Log(f"PWD(): attempt {attempt+1} failed. Exception={e}")
                if not self.Reconnect():
                    return ""
        else:
            return ""
        self.Log("PWD() --> '"+dir+"'")

        # Check to see if this matches what self._curdirpath thinks it ought to
        lead, tail=os.path.split(FTP.g_curdirpath)
        self.Log(f"PWD(): {lead=}  {tail=}")
        if not self.ComparePaths(FTP.g_curdirpath,  dir):
            Log(f"***PWD(): error detected -- self._curdirpath='{FTP.g_curdirpath}' and pwd returns '{dir}'")
            Log("***Probably irrecoverable, so exiting program.")
            raise NotImplementedError("This really shouldn't haev happened...")        # This should never happen.

        return dir


    # ---------------------------------------------
    # Given a complete path of the form "/xxx/yyy/zzz" (note leading "/"and no trailing "/"), or a relative path of the form "xxx" (note no slashes) does it exist?
    def PathExists(self, dirPath: str) -> bool:
        FTP._lastMessage=""  # Clear the last message
        dirPath=dirPath.replace("//", "/")

        dirPath=dirPath.strip()
        if len(dirPath) == 0:
            return False

        # Handle the case where we're looking at "/xxx", a folder at root level.
        if dirPath[0] == "/":
            if not self.CWD("/"):
                return False
            if dirPath[1:] == "":
                return True     # We asked for "/" with no file, so no need to check for file
            return self.FileExists(dirPath[1:])

        # Now deal with more complex paths
        path=dirPath.split("/")
        if len(path) == 0:
            return self.FileExists(dirPath)

        end=path[-1]    # The last element of the path
        rest="/".join(path[:-1])    # The beginning of the path
        if len(rest) > 0:
            self.CWD(rest)
        if end == "":
            return True
        return self.FileExists(end)


    # ---------------------------------------------
    # Given a filename (possibly including a complete path), does the file exist.  Note that a directory is treated as a file.
    def FileExists(self, filedir: str) -> bool:
        self.Log(f"FTP().FileExists('{filedir}') called")
        FTP._lastMessage=""  # Clear the last message
        if filedir == "/":
            self.Log(f"FileExists('{filedir}') --> of course it does.")
            return True     # "/" always exists

        # A trailing "/" needs to be ignored as that means there is no file, just a directory, and in that case, the "/" cause test to fail
        if filedir[-1] == "/":
            filedir=filedir[:-1]

        # Split the filedir into path+file
        path=""
        if "/" in filedir:
            path="/".join(filedir.split("/")[:-1])
            filedir=filedir.split("/")[-1]

        # Make sure we're at the path. SetDirectory walks it one component at a time (correct at any
        # depth) and leaves us in it. The older PathExists()+CWD() combination mishandled multi-component
        # *relative* paths -- it left the CWD at the parent and then re-applied the whole path relatively
        # -- which broke 3+ level paths such as a sub-page's "/series/con/subpage/...".
        if len(path) > 0:
            if not self.SetDirectory(path):
                self.Log(f"FileExists('{filedir}') --> path '{path}' does not exist")
                return False

        for attempt in range(2):
            try:
                if filedir in self.g_ftp.nlst():
                    self.Log(f"FileExists('{filedir}') --> yes")
                    return True
                self.Log(f"FileExists('{filedir}') --> no, it does not exist")
                return False
            except Exception:
                Log(f"FTP.FileExists(): attempt {attempt+1} failed: retrying check of {filedir}")
                if not self.Reconnect():
                    return False
        Log(f"FTP.FileExists(): failed after all attempts -- abandoning.")
        MessageBox(f"Two attempts to see if {filedir} exists failed.  Exiting program.")
        raise NotImplementedError("Beats the hell outta me what happened...")  # This should never happen.


    #-------------------------------
    # Make newdir (which may be a full path) the current working directory.  It (and the whole chain leading to it) may optionally be created if it does not exist.
    # Setting Create=True allows the creation of new directories as needed
    # Newdir can be a whole path starting with "/" or a path relative to the current directory if it doesn't start with a "/"
    def SetDirectory(self, newdir: str, Create: bool=False) -> bool:
        FTP._lastMessage=""  # Clear the last message
        self.Log(f"SetDirectory('{newdir}', {Create=})")

        # No directory means no work
        if newdir is None or len(newdir) == 0:
            return True

        # If we've been given an absolute path, and we're already there, return
        if newdir[0] == "/" and newdir == self.g_curdirpath:
            self.Log("SetDirectory: already there with an absolute path")
            return True

        components=[]
        if newdir[0] == "/":
            components.append("/")
            newdir=newdir[1:]
        components.extend(newdir.split("/"))
        components=[c.strip() for c in components if len(c) > 0]

        # Now walk the component list
        for component in components:
            # Does the directory exist?
            if not self.FileExists(component):
                # If not, are we allowed to create it"
                if not Create:
                    Log("***FTP.SetDirectory(): called for a non-existent directory with create=False")
                    return False
                if not self.MKD(component):
                    Log("***FTP.SetDirectory(): mkd failed...bailing out...")
                    return False

            # Now cwd to it.
            if not self.CWD(component):
                Log("***FTP.SetDirectory(): cwd failed...bailing out...")
                return False

        return True


    #-------------------------------
    # Copy the string s to fanac.org as a file in the current directory named fname.
    def PutString(self, fname: str, s: str) -> bool:
        FTP._lastMessage=""  # Clear the last message
        if self.g_ftp is None:
            Log("FTP.PutString(): FTP not initialized")
            return False

        data=bytes(s, 'utf-8')
        with tempfile.TemporaryFile() as f:

            # Save the string as a local temporary file, then rewind so it can be read
            f.write(data)
            f.seek(0)

            self.Log("STOR "+fname+"  from "+f.name)
            for attempt in range(2):
                f.seek(0)
                try:
                    ret=self.g_ftp.storbinary("STOR "+fname, f)
                    self.Log(ret)
                    if not self.IsSuccess(ret):
                        Log(f"FTP.PutString('{fname}'): server did not report success: {ret}", isError=True)
                        FTP._lastMessage=ret
                        return False
                    break
                except Exception as e:
                    Log(f"FTP.PutString(): attempt {attempt+1} failed. Exception={e}")
                    if not self.Reconnect():
                        return False
            else:
                return False
            # Make sure the server actually received all the bytes (catches zero-size/truncated uploads)
            return self.VerifyUploadedSize(fname, len(data))


    #-------------------------------
    # Append the string s to file fname on fanac.org in the current directory
    def AppendString(self, fname: str, s: str) -> bool:
        FTP._lastMessage=""  # Clear the last message
        if self.g_ftp is None:
            Log("FTP.AppendString(): FTP not initialized")
            return False

        with tempfile.TemporaryFile() as f:

            # Save the string as a local temporary file, then rewind so it can be read
            f.write(bytes(s, 'utf-8'))
            f.seek(0)

            self.Log("STOR "+fname+"  from "+f.name)
            for attempt in range(2):
                f.seek(0)
                try:
                    ret=self.g_ftp.storbinary("APPE "+fname, f)
                    self.Log(ret)
                    if not self.IsSuccess(ret):
                        Log(f"FTP.AppendString('{fname}'): server did not report success: {ret}", isError=True)
                        FTP._lastMessage=ret
                        return False
                    break
                except Exception as e:
                    Log(f"FTP.AppendString(): attempt {attempt+1} failed. Exception={e}")
                    if not self.Reconnect():
                        return False
            else:
                return False
            return True


    #-------------------------------
    # Copy the string s to fanac.org as a file <fname> in directory <directory>, creating directories as needed.
    def PutFileAsString(self, directory: str, fname: str, s: str, create: bool=False) -> bool:
        FTP._lastMessage=""  # Clear the last message
        if not FTP().SetDirectory(directory, Create=create):
            Log("FTP.PutFieAsString(): Bailing out...")
            return False
        return FTP().PutString(fname, s)

    # -------------------------------
    # Return True if a message is recognized as an FTP success message; False otherwise
    # Any 2xx completion reply is success (e.g. "226 Transfer complete", "226-File successfully transferred");
    # matching literal message text is too fragile across servers.
    def IsSuccess(self, ret: str) -> bool:
        if ret is None or ret == "":
            return False
        return ret.split("\n")[0].strip().startswith("2")


    # -------------------------------
    # After an upload, ask the server how big the file it received is and compare with what we sent.
    # This catches truncated transfers (e.g. zero-length files) that completed with a success reply.
    # If the server doesn't support SIZE, log it and pass -- the response-code check still applies.
    def VerifyUploadedSize(self, fname: str, expected: int) -> bool:
        try:
            actual=self.g_ftp.size(fname)
        except Exception as e:
            self.Log(f"VerifyUploadedSize: SIZE '{fname}' failed ({e}); skipping size verification")
            return True
        if actual is None:
            return True
        if actual != expected:
            msg=f"'{fname}' arrived on the server as {actual} bytes but should be {expected} bytes -- the upload was corrupted or truncated"
            Log("FTP.VerifyUploadedSize: "+msg, isError=True)
            FTP._lastMessage=msg
            return False
        return True


    #-------------------------------
    # Copy a file from one directory on the server to another
    def CopyFile(self, oldpathname: str, newpathname: str, filename: str, Create: bool=False) -> bool:
        return self.CopyAndRenameFile(oldpathname, filename, newpathname, Create=Create)


    #-------------------------------
    # Copy a file from one directory on the server to another. Rename the file if newfilename != ""
    def CopyAndRenameFile(self, oldpathname: str, oldfilename: str, newpathname: str, newfilename: str=None, Create: bool=False, IgnoreMissingFile: bool=False) -> bool:
        FTP._lastMessage=""  # Clear the last message
        if self.g_ftp is None:
            Log("FTP.CopyAndRenameFile(): FTP not initialized", isError=True)
            return False

        Log(f"CopyAndRenameFile: {oldpathname=} {oldfilename=} {newpathname=} {newfilename=}")

        self.CWD(oldpathname)

        # The lambda callback in retrbinary will accumulate bytes here
        temp: bytearray=bytearray(0)

        self.Log(f"RETR '{oldfilename}' from '{oldpathname}'")
        ret="**No message returned by retrbinary()**"
        for attempt in range(2):
            try:
                ret=self.g_ftp.retrbinary(f"RETR {oldfilename}", lambda data: temp.extend(data))
                self.Log(ret)
                break
            except error_perm as e:
                Log(f"FTP.CopyAndRenameFile().retrbinary(): attempt {attempt+1} failed: {e}", isError=True)
                if not self.Reconnect():
                    if IgnoreMissingFile:
                        return True
                    return False
        else:
            Log("FTP.CopyAndRenameFile(): retrbinary failed after all attempts", isError=True)
            return False

        if not self.IsSuccess(ret):
            Log(ret, isError=True)
            Log("FTP.CopyAndRenameFile(): retrbinary failed", isError=True)
            return False

        # Upload the file we just downloaded to the new directory, renaming it if specified.
        # The new directory must already have been created
        if not self.PathExists(newpathname):
            Log(f"FTP.CopyAndRenameFile(): newpathname='{newpathname}' not found", isError=True)
            if not Create:
                return False
            self.MKD(newpathname)
        self.CWD(newpathname)

        if newfilename is None:
            newfilename=oldfilename

        for attempt in range(2):
            try:
                ret=self.g_ftp.storbinary(f"STOR {newfilename}", io.BytesIO(temp))
                self.Log(ret)
                if not self.IsSuccess(ret):
                    Log(f"FTP.CopyAndRenameFile('{newfilename}'): server did not report success: {ret}", isError=True)
                    FTP._lastMessage=ret
                    return False
                break
            except Exception as e:
                Log(f"FTP.CopyAndRenameFile().storbinary(): attempt {attempt+1} failed. Exception={e}")
                if not self.Reconnect():
                    return False
        else:
            return False
        # Make sure the server actually received all the bytes (catches zero-size/truncated uploads)
        return self.VerifyUploadedSize(newfilename, len(temp))


    #-------------------------------
    # Make a timestamped copy of a file on the server
    # If the file does not exist, return True.
    # For other failures, return False
    def BackupServerFile(self, pathname) -> bool:
        FTP._lastMessage=""  # Clear the last message
        path, filename=os.path.split(pathname)
        if not FTP().SetDirectory(path, Create=True):
            Log(f"FTP.BackupServerFile(): Could not set directory to '{path}'")
            return False
        path=path.replace("//", "/")
        if not FTP().FileExists(filename):
            Log(f"BackupServerFile('{pathname}'): file does not exist, nothing to back up.")
            return True
        return FTP().CopyAndRenameFile(path, filename, path, TimestampFilename(filename))



    #-------------------------------
    # Copy the local file fname to fanac.org in the current directory and with the same thisrow
    def PutFile(self, pathname: str, toname: str) -> bool:
        FTP._lastMessage=""  # Clear the last message
        if self.g_ftp is None:
            Log("FTP.PutFile(): FTP not initialized")
            return False

        self.Log("STOR "+toname+"  from "+pathname)
        try:
            with open(pathname, "rb") as f:
                for attempt in range(2):
                    f.seek(0)
                    try:
                        ret=self.g_ftp.storbinary("STOR "+toname, f)
                        self.Log(ret)
                        if not self.IsSuccess(ret):
                            Log(f"FTP.PutFile('{toname}'): server did not report success: {ret}", isError=True)
                            FTP._lastMessage=ret
                            return False
                        break
                    except Exception as e:
                        Log(f"FTP.PutFile(): attempt {attempt+1} failed. Exception={e}")
                        if not self.Reconnect():
                            return False
                else:
                    return False
        except Exception as e:
            Log(f"FTP.PutFile(): Exception on Open('{pathname}', 'rb') ")
            Log(str(e))
            return False
        # Make sure the server actually received all the bytes (catches zero-size/truncated uploads)
        return self.VerifyUploadedSize(toname, os.path.getsize(pathname))


    #-------------------------------
    # Download the (binary) file named fname in directory on fanac.org to the local file localpath
    def GetFile(self, directory: str, fname: str, localpath: str) -> bool:
        FTP._lastMessage=""  # Clear the last message
        if self.g_ftp is None:
            Log("FTP.GetFile(): FTP not initialized")
            return False

        if not self.SetDirectory(directory):
            Log(f"***FTP.GetFile(): SetDirectory('{directory}') failed. Bailing out...")
            return False
        if not self.FileExists(fname):
            Log(f"FTP.GetFile(): '{fname}' does not exist.")
            return False

        self.Log("RETR "+fname+"  to "+localpath)
        msg=""
        try:
            with open(localpath, "wb") as f:
                for attempt in range(2):
                    f.seek(0)
                    f.truncate(0)
                    try:
                        msg=self.g_ftp.retrbinary("RETR "+fname, f.write)
                        break
                    except Exception as e:
                        Log(f"FTP.GetFile(): attempt {attempt+1} failed. Exception={e}")
                        if not self.Reconnect():
                            return False
                else:
                    return False
        except Exception as e:
            Log(f"FTP.GetFile(): Exception writing '{localpath}': {e}")
            return False
        self.Log(msg)
        return msg.startswith("226")


    #-------------------------------
    # Download the ascii file named fname in the current directory on fanac.org into a string
    def GetAsString(self, fname: str) -> str|None:
        FTP._lastMessage=""  # Clear the last message
        if self.g_ftp is None:
            Log("FTP.GetAsString(): FTP not initialized")
            return None

        fd=tempfile.TemporaryDirectory()
        self.Log("RETR "+fname+"  to "+fd.name)
        if not self.FileExists(fname):
            Log(f"FTP.GetAsString(): '{fname}' does not exist.")
            fd.cleanup()
            return None
        # Download the file into the temporary file
        tempfname=os.path.join(fd.name, "tempfile")
        f=open(tempfname, "wb+")
        msg=""
        for attempt in range(2):
            f.seek(0)
            f.truncate(0)
            try:
                msg=self.g_ftp.retrbinary("RETR "+fname, f.write)
                break
            except Exception as e:
                Log(f"FTP.GetAsString(): attempt {attempt+1} failed. Exception={e}")
                if not self.Reconnect():
                    fd.cleanup()
                    return None
        else:
            Log("FTP.GetAsString(): failed after all attempts")
            fd.cleanup()
            return None
        self.Log(msg)
        if not msg.startswith("226"):
            Log("FTP.GetAsString(): failed")
            fd.cleanup()
            return None

        with open(tempfname, "r", encoding='utf8') as f:
            out=f.readlines()
        out="".join(out)    # Rejoin into a string
        fd.cleanup()
        return out


    #-------------------------------
    def GetFileAsString(self, directory: str, fname: str, TestLoad: bool=False) -> str|None:
        FTP._lastMessage=""  # Clear the last message
        self.Log(f"GetFileAsString('{directory}', '{fname}')")
        if not self.SetDirectory(directory):
            if TestLoad:
                Log(f"***GetFileAsString(): SetDirectory('{directory}') not found. Not fatal.")
            else:
                Log(f"***GetFileAsString(): SetDirectory('{directory}') failed. Bailing out...")
            return None
        Log(f"GetFileAsString: {FTP().PWD()} / {fname}")
        s=FTP().GetAsString(fname)
        if s is None:
            Log(f"***FTP.GetFileAsString(): Could not load '{directory}/{fname}'")
        return s


    #-------------------------------
    def Nlst(self, directory: str) -> list[str]:
        FTP._lastMessage=""  # Clear the last message
        if self.g_ftp is None:
            Log("FTP.Nlst(): FTP not initialized")
            return []

        if not self.SetDirectory(directory):
            Log("FTP.Nlst(): Bailing out...")
            return []

        return [x for x in self.g_ftp.nlst() if x != "." and x != ".."] # Ignore the . and .. elements


#============================================================
# A class to maintain a crude locking system on an FTP server
# Note that an FTP link must already be set up.
class Lock:

    # Lock returns False if there is already a lock in place; returns True and sets a lock if there is no lock or the lock has expired
    def SetLock(self, path: str, id: str) -> str:

        lockid, lockdate=self.GetLock(path)
        if lockid == "":
            # There is none. So set a lock for me
            self.MakeLock(path, id)
            return ""

        # If a lock exists, but is my own id or is a blank id, we always override it and write a new lock.
        if lockid == id or lockid == "":
            self.MakeLock(path, id)
            return ""

        # If it's not my lock, see if it has expired
        lockdate=datetime.strptime(lockdate, '%Y-%m-%d %H:%M:%S')
        if datetime.now()-lockdate > timedelta(hours=12):
            # It has expired -- override it
            self.MakeLock(path, id)
            return ""

        # OK, it's locked by someone else
        return f"Locked by {lockid} on {lockdate:%Y-%m-%d} at {lockdate:%H:%M:%S}"


    def GetLock(self, path: str) -> tuple[str, str]:
        # Get any  existing lock
        lock=FTP().GetAsString(f"/{path}/Lock")
        if lock is None or lock == "":
            return ("", "")
        # There is an existing lock.  Extract the ID and datetime
        lockbits=lock.split("=", 1) + [""]  # The [" "] is to ensure there are always at least two elements in lockbits
        return (lockbits[0], lockbits[1])


    def MakeLock(self, path: str, id: str):
        if not FTP().PutString(f"/{path}/Lock", f"{id}={datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"):
            LogError(f"SetLock('{path}', '{id}') failed")
            raise Exception(f"SetLock('{path}', '{id}') failed")


    # Release my lock.
    # True indicates lock released (or never existed)
    # False indicates Classic is locked by someone else
    def ReleaseLock(self, path: str, id: str) -> bool:
        lock=FTP().GetAsString(path+"/Lock")

        if lock is None:
            return True

        lockid, lockdate=lock.split("=", 1)

        # If it's my own lock, we always override it.  Otherwise, we always leave it.
        if lockid == id:
            if FTP().DeleteFile(path+"/Lock"):
                return True

        return False



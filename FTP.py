from __future__ import annotations
import ftplib
from ftplib import error_perm
import json
import os
import tempfile
import io

from win32ctypes.pywin32.pywintypes import datetime
from datetime import datetime, timedelta

from Log import Log, LogFlush, LogError
from HelpersPackage import TimestampFilename


class FTP:
    g_ftp: ftplib.FTP=None      # A single FTP link for all instances of the class
    g_curdirpath: str="/"
    g_credentials: dict={}      # Saves the credentials for reconnection if the server times out
    g_dologging: bool=True      # Turn on logging of useful debugging information
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
    def Log(self, s: str, noNewLine=False):
        if FTP.g_dologging:
            Log(s, noNewLine)


    def LoggingOff(self):
        if FTP.g_dologging:
            Log("FTP Logging turned off")    # Only log a change of state
        FTP.g_dologging = False


    def LoggingOn(self):
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
        FTP.g_ftp=ftplib.FTP_TLS(host=FTP.g_credentials["host"], user=FTP.GetEditor(), passwd=FTP.g_credentials["PW"])
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
        try:
            msg=self.g_ftp.cwd(newdir)
        except Exception as e:
            self.Log(f"***FTP.CWD(): FTP connection failure. Exception={e}")
            if not self.Reconnect():
                return False
            try:
                msg=self.g_ftp.cwd(newdir)
            except Exception as e:
                self.Log(f"***g_ftp.cwd('{newdir}'): FTP connection failure. Exception={e}")
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
        try:
            msg=self.g_ftp.mkd(newdir)
        except Exception as e:
            Log("FTP.MKD(): FTP connection failure. Exception="+str(e))
            if not self.Reconnect():
                return False
            msg=self.g_ftp.mkd(newdir)
        self.Log(msg+"\n")
        return msg == newdir or msg.startswith("250 ") or msg.startswith("257 ")     # Web doc shows all three as possible.


    # ---------------------------------------------
    def DeleteFile(self, fname: str) -> bool:
        FTP._lastMessage=""   # Clear the last essage
        self.Log("**delete file: '"+fname+"'")
        if len(fname.strip()) == 0:
            Log("FTP.DeleteFile(): filename not supplied.")
            LogFlush()
            assert False

        if not self.FileExists(fname):
            Log("FTP.DeleteFile: '"+fname+"' does not exist.")
            return True

        try:
            msg=self.g_ftp.delete(fname)
        except Exception as e:
            Log("FTP connection failure. Exception="+str(e))
            if not self.Reconnect():
                return False
            msg=self.g_ftp.delete(fname)
        self.Log(msg+"\n")
        return msg.startswith("250 ")


    # ---------------------------------------------
    def Rename(self, oldname: str, newname: str) -> bool:
        FTP._lastMessage=""   # Clear the last message
        self.Log(f"**rename file: '{oldname}'  as  '{newname}'")
        if len(oldname.strip()) == 0 or len(newname.strip()) == 0:
            Log("FTP.Rename(): oldname or newname not supplied. Probably irrecoverable, so exiting program.")
            LogFlush()
            assert False

        if not self.FileExists(oldname):
            msg=f"FTP.Rename: '{oldname}' does not exist."
            FTP._lastMessage=msg
            Log(msg)
            return False

        try:
            msg=self.g_ftp.rename(oldname, newname)
            FTP._lastMessage=msg
        except Exception as e:
            Log(f"FTP.Rename: FTP connection failure. Exception={e}")
            if not self.Reconnect():
                return False
            msg=self.g_ftp.rename(oldname, newname)
            FTP._lastMessage=msg
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
            assert False        # This should never happen.
        if dirname == "/":
            Log("FTP.DeleteDir(): Attempt to delete root -- forbidden")
            assert False

        if not self.FileExists(dirname):
            Log(f"FTP.DeleteDir(): '{dirname}' does not exist.")
            return True

        # The first step is to delete any files it contains
        files=self.Nlst(dirname)
        for file in files:
            self.DeleteFile(file)

        try:
            msg=self.g_ftp.rmd(dirname)
            FTP._lastMessage=msg
        except Exception as e:
            Log(f"FTP.DeleteDir(): FTP connection failure. Exception={e}")
            if not self.Reconnect():
                return False
            msg=self.g_ftp.rmd(dirname)
            FTP._lastMessage=msg

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
        try:
            dir=self.g_ftp.pwd()
        except Exception as e:
            Log("PWD(): FTP connection failure. Exception="+str(e))
            if not self.Reconnect():
                return ""
            dir=self.g_ftp.pwd()
        self.Log("PWD() --> '"+dir+"'")

        # Check to see if this matches what self._curdirpath thinks it ought to
        lead, tail=os.path.split(FTP.g_curdirpath)
        self.Log(f"PWD(): {lead=}  {tail=}")
        if not self.ComparePaths(FTP.g_curdirpath,  dir):
            Log(f"***PWD(): error detected -- self._curdirpath='{FTP.g_curdirpath}' and pwd returns '{dir}'")
            Log("***Probably irrecoverable, so exiting program.")
            assert False

        return dir


    # ---------------------------------------------
    # Given a complete path of the form "/xxx/yyy/zzz" (note leading "/"and no trailing "/"), or a relative path of the form "xxx" (note no slashes) does it exist?
    def PathExists(self, dirPath: str) -> bool:
        FTP._lastMessage=""  # Clear the last message
        FTP._lastMessage=""   # Clear the last message
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
    # Given a filename (possibly includeing a complete path), does the file exist.  Note that a directory is treated as a file.
    def FileExists(self, filedir: str) -> bool:
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

        # Make sure we're at the path
        if len(path) > 0:
            if not self.PathExists(path):
                self.Log(f"FileExists('{filedir}') --> path '{path}' does not exist")
                return False
            self.CWD(path)

        try:
            if filedir in self.g_ftp.nlst():
                self.Log(f"FileExists('{filedir}') --> yes")
                return True
            self.Log(f"FileExists('{filedir}') --> no, it does not exist")
            return False
        except:
            Log("'FTP.FileExists(): FTP failure: retrying")
            if not self.Reconnect():
                return False
            return self.FileExists(filedir)


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

        with tempfile.TemporaryFile() as f:

            # Save the string as a local temporary file, then rewind so it can be read
            f.write(bytes(s, 'utf-8'))
            f.seek(0)

            self.Log("STOR "+fname+"  from "+f.name)
            try:
                self.Log(self.g_ftp.storbinary("STOR "+fname, f))
            except Exception as e:
                Log(f"FTP.PutString(): FTP connection failure. Exception={e}")
                if not self.Reconnect():
                    return False
                self.Log(self.g_ftp.storbinary("STOR "+fname, f))
            return True


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
            try:
                self.Log(self.g_ftp.storbinary("APPE "+fname, f))
            except Exception as e:
                Log(f"FTP.AppendString(): FTP connection failure. Exception={e}")
                if not self.Reconnect():
                    return False
                self.Log(self.g_ftp.storbinary("APPE "+fname, f))
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
    def IsSuccess(self, ret: str) -> bool:
        successMessages=[
            "226-File successfully transferred",
        ]
        ret=ret.split("\n")[0]      # Just want the 1st line if there are many
        return any([x == ret for x in successMessages])


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
        ret="No message returned by retrbinary()"
        try:
            ret=self.g_ftp.retrbinary(f"RETR {oldfilename.replace(' ', '%20')}", lambda data: temp.extend(data))
            self.Log(ret)
        except error_perm as e:
            Log(ret)
            Log(f"FTP.CopyAndRenameFile().retrbinary(): Exception={e}", isError=True)
            if not self.Reconnect():
                if IgnoreMissingFile:
                    return True
                return False
            ret=self.g_ftp.retrbinary(f"RETR {oldfilename}", lambda data: temp.extend(data))
            self.Log(ret)

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

        try:
            ret=self.g_ftp.storbinary(f"STOR {newfilename}", io.BytesIO(temp))
            self.Log(ret)
        except Exception as e:
            Log(f"FTP.CopyAndRenameFile().PutFile(): FTP connection failure. Exception={e}")
            if not self.Reconnect():
                return False
            ret=self.g_ftp.storbinary(f"STOR {newfilename}", io.BytesIO(temp))
            self.Log(ret)
        return True


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
        try:
            return FTP().CopyAndRenameFile(path, filename, path, TimestampFilename(filename))
        except error_perm as e:
            Log(f"BackupServerFile('{pathname}'): could not read file to be backed up.  Will assume there is nothing needing backup.")
        return True



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
                try:
                    self.Log(self.g_ftp.storbinary("STOR "+toname, f))
                except Exception as e:
                    Log("FTP.PutFile(): FTP connection failure. Exception="+str(e))
                    if not self.Reconnect():
                        return False
                    self.Log(self.g_ftp.storbinary("STOR "+toname, f))
        except Exception as e:
            Log(f"FTP.PutFile(): Exception on Open('{pathname}', 'rb') ")
            Log(str(e))
            return False
        return True


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
        try:
            msg=self.g_ftp.retrbinary("RETR "+fname, f.write)
        except Exception as e:
            Log(f"FTP.GetAsString(): FTP connection failure. Exception={e}")
            if not self.Reconnect():
                fd.cleanup()
                return None
            msg=self.g_ftp.retrbinary("RETR "+fname, f.write)
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
# A class to maintain acrude locking system on an FTP server
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



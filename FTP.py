from __future__ import annotations
from typing import Optional

import ftplib
import json
import os
import tempfile
import io

from Log import Log, LogFlush


class FTP:
    g_ftp: ftplib.FTP=None      # A single FTP link for all instances of the class
    g_curdirpath: str="/"
    g_credentials: dict={}      # Saves the credentials for reconnection if the server times out

    # ---------------------------------------------
    def OpenConnection(self, credentialsFilePath: str) -> bool:
        with open(credentialsFilePath) as f:
            FTP.g_credentials=json.loads(f.read())
        return self.Reconnect()     # Not exactly a reconnect, but close enough...

    #----------------------------------------------
    def GetEditor(self) -> str:
        return FTP.g_credentials["ID"]

    # ---------------------------------------------
    # If we get a connection failure, reconnect tries to re-establish the connection and put the FTP object into a consistent state and then to restore the CWD
    def Reconnect(self) -> bool:
        Log("Reconnect")
        if len(FTP.g_credentials) == 0:
            return False
        FTP.g_ftp=ftplib.FTP_TLS(host=FTP.g_credentials["host"], user=FTP.g_credentials["ID"], passwd=FTP.g_credentials["PW"])
        FTP.g_ftp.prot_p()

        # Now we need to restore the current working directory
        Log("Reconnect: g_ftp.cwd('/')")
        msg=self.g_ftp.cwd("/")
        Log(msg)
        ret=msg.startswith("250 OK.")
        if not ret:
            Log("Reconnect failed")
            return False

        Log("Reconnect: successful. Change directory to "+FTP.g_curdirpath)
        olddir=FTP.g_curdirpath
        FTP.g_curdirpath="/"
        self.SetDirectory(olddir)

        return True

    # ---------------------------------------------
    # Update the saved current working directory path
    # If the input is an absolute path, just use it (removing any trailing filename)
    # If it's a relative move, compute the new wd path
    def UpdateCurpath(self, newdir: str) -> None:
        Log("UpdateCurpath from "+FTP.g_curdirpath+"  with cwd('"+newdir+"')")
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


    #---------------------------------------------
    def CWD(self, newdir: str) -> bool:
        wd=self.PWD()
        Log("**CWD from '"+wd+"' to '"+newdir+"'")
        if wd == newdir:
            Log("  Already there!")
            return True

        try:
            msg=self.g_ftp.cwd(newdir)
        except Exception as e:
            Log("FTP connection failure. Exception="+str(e))
            if not self.Reconnect():
                return False
            msg=self.g_ftp.cwd(newdir)

        Log(msg)
        ret=msg.startswith("250 OK.")
        if ret:
            self.UpdateCurpath(newdir)
        self.PWD()
        return ret

    # ---------------------------------------------
    def MKD(self, newdir: str) -> bool:
        Log("**make directory: '"+newdir+"'")
        try:
            msg=self.g_ftp.mkd(newdir)
        except Exception as e:
            Log("FTP connection failure. Exception="+str(e))
            if not self.Reconnect():
                return False
            msg=self.g_ftp.mkd(newdir)
        Log(msg+"\n")
        return msg == newdir or msg.startswith("250 ") or msg.startswith("257 ")     # Web doc shows all three as possible.

    # ---------------------------------------------
    def DeleteFile(self, fname: str) -> bool:
        Log("**delete file: '"+fname+"'")
        if len(fname.strip()) == 0:
            Log("FTP.DeleteFile: filename not supplied.")
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
        Log(msg+"\n")
        return msg.startswith("250 ")

    # ---------------------------------------------
    def Rename(self, oldname: str, newname: str) -> bool:
        Log("**rename file: '"+oldname+"'  as  '"+newname+"'")
        if len(oldname.strip()) == 0 or len(newname.strip()) == 0:
            Log("FTP.Rename: oldname or newname not supplied.")
            LogFlush()
            assert False

        if not self.FileExists(oldname):
            Log("FTP.Rename: '"+oldname+"' does not exist.")
            return False

        try:
            msg=self.g_ftp.rename(oldname, newname)
        except Exception as e:
            Log("FTP connection failure. Exception="+str(e))
            if not self.Reconnect():
                return False
            msg=self.g_ftp.rename(oldname, newname)
        Log(msg+"\n")
        return msg.startswith("250 ")

    # ---------------------------------------------
    # Note that this does not delete recursively.
    def DeleteDir(self, dirname: str) -> bool:
        Log("**delete directory: '"+dirname+"'")
        if len(dirname.strip()) == 0:
            Log("FTP.DeleteDir: dirname not supplied.")
            LogFlush()
            assert False        # This should never happen.
        if dirname == "/":
            Log("FTP.DeleteDir: Attempt to delete root -- forbidden")
            assert False

        if not self.FileExists(dirname):
            Log("FTP.DeleteDir: '"+dirname+"' does not exist.")
            return True

        # The first step is to delete any files it contains
        files=self.Nlst(dirname)
        for file in files:
            self.DeleteFile(file)

        try:
            msg=self.g_ftp.rmd(dirname)
        except Exception as e:
            Log("FTP connection failure. Exception="+str(e))
            if not self.Reconnect():
                return False
            msg=self.g_ftp.rmd(dirname)
        Log(msg+"\n")
        return msg.startswith("250 ")

    # ---------------------------------------------
    def PWD(self) -> str:
        try:
            dir=self.g_ftp.pwd()
        except Exception as e:
            Log("FTP connection failure. Exception="+str(e))
            if not self.Reconnect():
                return False
            dir=self.g_ftp.pwd()
        Log("pwd is '"+dir+"'")

        # Check to see if this matches what self._curdirpath thinks it ought to
        _, tail=os.path.split(FTP.g_curdirpath)
        if FTP.g_curdirpath != dir and tail != dir:
            Log("PWD: error detected -- self._curdirpath='"+FTP.g_curdirpath+"' and pwd returns '"+dir+"'")
            assert False

        return dir


    # ---------------------------------------------
    def PathExists(self, dirPath: str) -> bool:
        path=dirPath.split("/")
        if len(path) == 0:
            return self.FileExists(dirPath)

        dir=path[-1]
        self.CWD("/".join(path[:-1]))
        return self.FileExists(dir)


    # ---------------------------------------------
    def FileExists(self, filedir: str) -> bool:
        Log("Does '"+filedir+"' exist?", noNewLine=True)
        if filedir == "/":
            Log("  --> Yes, it always exists")
            return True     # "/" always exists

        # Split the filedir into path+file
        path=""
        if "/" in filedir:
            path="/".join(filedir.split("/")[:-1])
            filedir=filedir.split("/")[-1]
        # Make sure we're at the path
        if len(path) > 0:
            self.CWD(path)

        try:
            if filedir in self.g_ftp.nlst():
                Log("  --> yes")
                return True
            Log("'  --> no, it does not exist")
            return False
        except:
            Log("'  --> FTP failure: retrying")
            if not self.Reconnect():
                return False
            return self.FileExists(filedir)


    #-------------------------------
    # Setting Create=True allows the creation of new directories as needed
    # Newdir can be a whole path starting with "/" or a path relative to the current directory if it doesn't starts with a "/"
    def SetDirectory(self, newdir: str, Create: bool=False) -> bool:
        Log("**SetDirectory: "+newdir)

        # Split newdir into components
        if newdir is None or len(newdir) == 0:
            return True

        # If we've been given an absolte path and we're already there, return
        if newdir[0] == "/" and newdir == self.g_curdirpath:
            Log("SetDirectory: already there with an absolute path")
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
                    Log("SetDirectory was called for a non-existant directory with create=False")
                    return False
                if not self.MKD(component):
                    Log("mkd failed...bailing out...")
                    return False

            # Now cwd to it.
            if not self.CWD(component):
                Log("cwd failed...bailing out...")
                return False

        return True


    #-------------------------------
    # Copy the string to fanac.org in the current directory as fname
    def PutString(self, fname: str, s: str) -> bool:
        if self.g_ftp is None:
            Log("FTP not initialized")
            return False

        with tempfile.TemporaryFile() as f:

            # Save the string as a local temporary file, then rewind so it can be read
            f.write(bytes(s, 'utf-8'))
            f.seek(0)

            Log("STOR "+fname+"  from "+f.name)
            try:
                Log(self.g_ftp.storbinary("STOR "+fname, f))
            except Exception as e:
                Log("FTP connection failure. Exception="+str(e))
                if not self.Reconnect():
                    return False
                Log(self.g_ftp.storbinary("STOR "+fname, f))
            return True


    #-------------------------------
    # Append the string to file fname on fanac.org in the current directory
    def AppendString(self, fname: str, s: str) -> bool:
        if self.g_ftp is None:
            Log("FTP not initialized")
            return False

        with tempfile.TemporaryFile() as f:

            # Save the string as a local temporary file, then rewind so it can be read
            f.write(bytes(s, 'utf-8'))
            f.seek(0)

            Log("STOR "+fname+"  from "+f.name)
            try:
                Log(self.g_ftp.storbinary("APPE "+fname, f))
            except Exception as e:
                Log("FTP connection failure. Exception="+str(e))
                if not self.Reconnect():
                    return False
                Log(self.g_ftp.storbinary("APPE "+fname, f))
            return True


    #-------------------------------
    def PutFileAsString(self, directory: str, fname: str, s: str, create: bool=False) -> bool:
        if not FTP().SetDirectory(directory, Create=create):
            Log("PutFieAsString: Bailing out...")
            return False
        return FTP().PutString(fname, s)


    # Return True if a message is recognized as an FTP success message; False otherwise
    def IsSuccess(self, ret: str) -> bool:
        successMessages=[
            "226-File successfully transferred",
        ]
        ret=ret.split("\n")[0]      # Just want the 1st line if there are many
        return any([x == ret for x in successMessages])


    #-------------------------------
    # Copy a file from one directory on the the server to another
    def CopyFile(self, oldpathname: str, newpathname: str, filename: str) -> bool:
        if self.g_ftp is None:
            Log("FTP.CopyFile: FTP not initialized", isError=True)
            return False

        if not self.PathExists(oldpathname):
            Log("FTP.CopyFile: oldpathname '"+oldpathname+"' not found", isError=True)
            return False
        self.CWD(oldpathname)

        # The lambda callback in retrbinary will accumulate bytes here
        temp: bytearray=bytearray(0)

        Log("RETR "+filename+"  from "+oldpathname)
        try:
            ret=self.g_ftp.retrbinary("RETR "+filename, lambda data: temp.extend(data))
            Log(ret)
        except Exception as e:
            Log("FTP.CopyFile: FTP connection failure. Exception="+str(e), isError=True)
            if not self.Reconnect():
                return False
            ret=self.g_ftp.retrbinary("RETR "+filename, lambda data: temp.extend(data))
            Log(ret)

        if not self.IsSuccess(ret):
            Log(ret, isError=True)
            Log("FTP.CopyFile: retrbinary failed", isError=True)

        # Write upload the file we just downloaded to the new directory
        # The new directory must already have been created
        if not self.PathExists(newpathname):
            Log(f"FTP.CopyFile: newpathname='{newpathname}' not found", isError=True)
            return False
        self.CWD(newpathname)

        try:
            Log(self.g_ftp.storbinary("STOR "+filename, io.BytesIO(temp)))
        except Exception as e:
            Log(f"FTP.PutFile: FTP connection failure. Exception={e}")
            if not self.Reconnect():
                return False
            Log(self.g_ftp.storbinary("STOR "+filename, f))
        return True

    #-------------------------------
    # Copy the local file fname to fanac.org in the current directory and with the same name
    def PutFile(self, pathname: str, toname: str) -> bool:
        if self.g_ftp is None:
            Log("FTP.PutFile: FTP not initialized")
            return False

        Log("STOR "+toname+"  from "+pathname)
        try:
            with open(pathname, "rb") as f:
                try:
                    Log(self.g_ftp.storbinary("STOR "+toname, f))
                except Exception as e:
                    Log("FTP.PutFile: FTP connection failure. Exception="+str(e))
                    if not self.Reconnect():
                        return False
                    Log(self.g_ftp.storbinary("STOR "+toname, f))
        except Exception as e:
            Log(f"FTP.PutFile: Exception on Open('{pathname}', 'rb') ")
            Log(str(e))
        return True


    #-------------------------------
    # Download the ascii file named fname in the current directory on fanac.org into a string
    def GetAsString(self, fname: str) -> Optional[str]:
        if self.g_ftp is None:
            Log("FTP not initialized")
            return None

        fd=tempfile.TemporaryDirectory()
        Log("RETR "+fname+"  to "+fd.name)
        if not self.FileExists(fname):
            Log(f"{fname} does not exist.")
            fd.cleanup()
            return None
        # Download the file into the temporary file
        tempfname=os.path.join(fd.name, "tempfile")
        f=open(tempfname, "wb+")
        try:
            msg=self.g_ftp.retrbinary("RETR "+fname, f.write)
        except Exception as e:
            Log(f"FTP connection failure. Exception={e}")
            if not self.Reconnect():
                fd.cleanup()
                return None
            msg=self.g_ftp.retrbinary("RETR "+fname, f.write)
        Log(msg)
        if not msg.startswith("226-File successfully transferred"):
            Log("GetAsString failed")
            fd.cleanup()
            return None

        with open(tempfname, "r") as f:
            out=f.readlines()
        out="/n".join(out)
        fd.cleanup()
        return out


    #-------------------------------
    def GetFileAsString(self, directory: str, fname: str) -> Optional[str]:
        if not self.SetDirectory(directory):
            Log("GetFileAsString: Bailing out...")
            return None
        s=FTP().GetAsString(fname)
        if s is None:
            Log(f"Could not load {directory}/{fname}")
        return s



    #-------------------------------
    def Nlst(self, directory: str) -> list[str]:
        if self.g_ftp is None:
            Log("FTP.Nlst: FTP not initialized")
            return []

        if not self.SetDirectory(directory):
            Log("FTP.Nlst: Bailing out...")
            return []

        return [x for x in self.g_ftp.nlst() if x != "." and x != ".."] # Ignore the . and .. elements

function [fileinfo,filenumber] = Read_Dir_Files(inputdir,extention)
% This function read the files with all the input extensions in the directory 'inputdir' 
% Input: inputdir  --- the directory you would like to go through
%        extention --- cells, have the extension you specified
% Output: fileinfo --- cells, fileinfo{i}.fullname fileinfo{i}.name
%                      fileinfo{i}.extension
%         filenumber -- the total number of[fileinfo,filenumber] = Read_Dir_Files(inputdir,extention) files
%
% Example usage: [fileinfo,filenumber] = Read_Dir_Files('./',{'.m'})
%


files = dir(inputdir);  % get all the name of files and dirs under the folder 'inputdir'
% Here files can be file or a directory


%Loop over all filenames
ii = 0;
for iF = 1:length(files)
    %
    if (~files(iF).isdir)
        %
        % If name is not a directory get detailed information
        %
        [pathstr, name, ext] = fileparts(files(iF).name);
        %     [PATHSTR,NAME,EXT,VERSN] = FILEPARTS(FILE) returns the path,
        %     filename, extension and version for the specified file.
        %     FILEPARTS is platform dependent.
        
        %Loop for all the extension
        for ie=1:length(extention)
            %
            % Compare to known extentions and add to list
            %
            temp = extention{ie};
            extensionL = length(temp);
            if (strcmpi(extention{ie},[name(end-(extensionL-length(ext)-1):end) ext]))
                ii = ii + 1;
                ext = ext(2:end);
                fileinfo{ii}.fullname   = [inputdir,name,'.',ext];
                fileinfo{ii}.name       = name;
                fileinfo{ii}.extension  = ext;
            end
            %
        end
        %
    end
    %
end
filenumber = ii;
if filenumber==0
    fileinfo={};
end

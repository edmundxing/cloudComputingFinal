% =========================================================================
% An example code for the algorithm proposed in
%
%   Jinjun Wang, Jianchao Yang, Kai Yu, Fengjun Lv, Thomas Huang, and Yihong Gong.
%   "Locality-constrained Linear Coding for Image Classification", CVPR 2010.
%
%
% Written by Jianchao Yang @ IFP UIUC
% May, 2010.
% =========================================================================

function fdatabase = LLC_SPM_coding(dict_path,data_dir,fea_dir)

% -------------------------------------------------------------------------
% parameter setting
pyramid = [1, 2, 4];                % spatial block structure for the SPM
knn = 5;                            % number of neighbors for local coding

% nRounds = 10;                       % number of random test on the dataset
% tr_num  = 30;                       % training examples per category
% mem_block = 3000;                   % maxmum number of testing features loaded each time  


% -------------------------------------------------------------------------
% set path                        
%data_dir = 'data/Caltech101';       % directory for saving SIFT descriptors
%fea_dir = 'features/Caltech101';    % directory for saving final image features

% -------------------------------------------------------------------------
% extract SIFT descriptors, we use Prof. Lazebnik's matlab codes in this package
% change the parameters for SIFT extraction inside function 'extr_sift'
% extr_sift(img_dir, data_dir);

% -------------------------------------------------------------------------
% retrieve the directory of the database and load the codebook
database = retr_database_dir_spmllc(data_dir);

if isempty(database),
    error('Data directory error!');
end

load(dict_path);
nCodebook = size(B, 2);              % size of the codebook

% -------------------------------------------------------------------------
% extract image features

dFea = sum(nCodebook*pyramid.^2);
nFea = length(database.path);

fdatabase = struct;
fdatabase.path = cell(nFea, 1);         % path for each image feature
fdatabase.label = zeros(nFea, 1);       % class label for each image feature

for iter1 = 1:nFea,  
    if ~mod(iter1, 5),
       fprintf('.');
    end
    if ~mod(iter1, 100),
        fprintf(' %d images processed\n', iter1);
    end
    fpath = database.path{iter1};
    flabel = database.label(iter1);
    
    load(fpath);
    [rtpath, fname] = fileparts(fpath);
    feaPath = fullfile(fea_dir, num2str(flabel), [fname '.mat']);
    
 
    fea = LLC_pooling(feaSet, B, pyramid, knn);
    label = database.label(iter1);

    if ~isdir(fullfile(fea_dir, num2str(flabel))),
        mkdir(fullfile(fea_dir, num2str(flabel)));
    end      
    save(feaPath, 'fea', 'label');

    
    fdatabase.label(iter1) = flabel;
    fdatabase.path{iter1} = feaPath;
end;
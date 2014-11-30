function mainGetSPMLLC(dataclass)

% read files
image_dir = ['../lung_data/' dataclass]; 
ext = 'png';
pathData = 'LLC-SPM';
data_dir = [pathData '\sift_' dataclass];
if (~exist(pathData))
    mkdir(pathData);
end

inputData = ['inputData_' dataclass '.mat'];
if(~exist([pathData '\' inputData]))
    database = retr_database_dir(image_dir, ext);
    save([pathData '\' inputData],'database');
else
    load([pathData '\' inputData]);
end

%% learn codebook
redofeature = 0;
if(redofeature==1)
    databaseFeature = extr_sift(image_dir, data_dir, 'png');
    save([pathData '\sift_' dataclass '_dataFeature.mat'], 'databaseFeature');
else
    load([pathData '\sift_' dataclass '_dataFeature.mat']);
end
database.featurepath = databaseFeature.path;
B = learnDictionaryLLC(pathData, database.featurepath([1 301 500 801]), 'kmeans', 512);
save([pathData '\dictionary_' dataclass '\spmllc-ditionary.mat'],'B');

%% llc coding
fea_dir = [pathData '\features_' dataclass];
llcdatabase = LLC_SPM_coding([pathData '\dictionary_' dataclass '\spmllc-ditionary.mat'],data_dir,fea_dir);

[spmllcDescriptor spmllcDescriptorLabel] = getAllFeatures(llcdatabase.path);
spmllcDescriptor = normr(spmllcDescriptor);
filepath = database.path;
save([pathData '\spmllcDescriptor_' dataclass '.mat'],'spmllcDescriptor','spmllcDescriptorLabel','filepath','llcdatabase');
save([pathData '\' inputData],'database');
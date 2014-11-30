function B = learnDictionaryLLC(featurePathNames, method, dictSize)

% learn codebook
% input:
%   featurePathNames: feature file name
%   method: method used to learn the codebook
%   dictSize: the size of the codebook
% output:
%   B: the learned codebook

features = [];
for isp = 1:length(featurePathNames)
    load(featurePathNames{isp});
    features = [features feaSet.feaArr];
end

switch method
    case 'none' % all sample as dictionary
        B = features;
    case 'kmeans'
        [idx,dictionary] = kmeans(features',dictSize);     
        B = dictionary';
    otherwise
        error('Not supported dictionary learning method.');
end

%save([pathData '\dictionary\spmllc-ditionary.mat'],'B');
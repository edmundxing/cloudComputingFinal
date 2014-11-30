function [spmllcDescriptor spmllcDescriptorLabel] = getAllFeatures(llcfeaturepath)

% get all features for retrieval
% input: 
%   llcfeaturepath: llc code path
% output:
%   spmllcDescriptor: all llc codes
%   spmllcDescriptorLabel: image labels

llcFeatures = [];
llcLabels = [];
for isp = 1:length(llcfeaturepath)
    load(llcfeaturepath{isp});
    llcFeatures = [llcFeatures fea];
    llcLabels = [llcLabels label];
end

spmllcDescriptor= llcFeatures';
spmllcDescriptorLabel= llcLabels';
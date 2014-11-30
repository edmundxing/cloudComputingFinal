function [spaQueryHistMatrixNorm, spaQueryHistMatrix]= compSegmentAnnularHistogram(QueryImg, HistBin, Kbin, Ksegment)

% compute refined annular intensity historgam
% input:
%   QueryImg: input image
%   HistBin: the number of bins for the histogram
%   Kbin: the number of annular histogram
%   Ksegment: the number of segments
% output:
%   spaQueryHistMatrixNorm: normalized annular histogram
%   spaQueryHistMatrix: annular histogram

if (length(HistBin) == 1)
    % euqally spacing
    binStep = round(256/HistBin);
    HistBin = 0:binStep:binStep*HistBin; % has one more bin, the last bin is redundant
end

numBin = length(HistBin) - 1;
[Height, Width] = size(QueryImg(:,:,1));

QueryHistMatrix = zeros(size(QueryImg, 3)*numBin, Kbin*Ksegment);
QueryHistMatrixNorm = zeros(size(QueryImg, 3)*numBin, Kbin*Ksegment);
% WidthBinX = floor((TestPos(1,2)-TestPos(1,1))/(2*Kbin));
% WidthBinY = floor((TestPos(2,2)-TestPos(2,1))/(2*Kbin));
WidthBinX = floor((Width-1)/(2*Kbin));
WidthBinY = floor((Height-1)/(2*Kbin));
for iSeg = 1:Ksegment
    xA = [1 Width]; xB = xA;
    yA = [1 Height]; yB = yA;
    SegPatch = PatchSegment(QueryImg, xA, yA, Ksegment, iSeg);
    for iKbin = 1:Kbin
        TestHist = zeros(numBin*size(QueryImg, 3), 1);
        %TestHistNorm = zeros(numBin*size(QueryImg, 3), 1);
        if (iKbin == Kbin)
            ChnImgBW = zeros(size(QueryImg(:,:,1)));
            ChnImgBW(yA(1):yA(2), xA(1):xA(2)) = 1;
            ChnImgBW = (ChnImgBW & SegPatch);
            for iChn = 1:size(QueryImg, 3)
                ChnImg = QueryImg(:,:,iChn);
                vhist = ChnImg(ChnImgBW == 1);
                shist = histc(vhist(:), HistBin); shist(end) = [];
                TestHist(numBin*(iChn-1)+1:numBin*iChn) = shist';
                %shist = shist/sum(shist); % may be normalize after concatenate
                %TestHistNorm(numBin*(iChn-1)+1:numBin*iChn) = shist';
            end
        else
            xB = [xA(1)+WidthBinX xA(2)-WidthBinX];
            yB = [yA(1)+WidthBinY yA(2)-WidthBinY];
            ChnImgBW = zeros(size(QueryImg(:,:,1)));
            ChnImgBW(yA(1):yA(2), xA(1):xA(2)) = 1;
            ChnImgBW(yB(1):yB(2), xB(1):xB(2)) = 0;
            ChnImgBW = (ChnImgBW & SegPatch);
            for iChn = 1:size(QueryImg, 3)
                ChnImg = QueryImg(:,:,iChn);
                vhist = ChnImg(ChnImgBW == 1);
                shist = histc(vhist(:), HistBin); shist(end) = [];
                TestHist(numBin*(iChn-1)+1:numBin*iChn) = shist';
                %shist = shist/sum(shist); % may be normalize after concatenate
                %TestHistNorm(numBin*(iChn-1)+1:numBin*iChn) = shist';
            end
        end     
        TestHistNorm = TestHist/sum(TestHist(:));
        TestHistNorm(isnan(TestHistNorm)) = 0;
        QueryHistMatrix(:,(iSeg - 1)*Kbin + Kbin - iKbin + 1) = TestHist;
        QueryHistMatrixNorm(:,(iSeg - 1)*Kbin + Kbin - iKbin + 1) = TestHistNorm;
        xA = xB;
        yA = yB;
    end    
end

spaQueryHistMatrix = sparse(QueryHistMatrix(:));
spaQueryHistMatrixNorm = sparse(QueryHistMatrixNorm(:));
clear QueryHistMatrixNorm QueryHistMatrix TestHist
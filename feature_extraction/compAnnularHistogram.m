function [spaQueryHistMatrixNorm, spaQueryHistMatrix] = compAnnularHistogram(QueryImg, HistBin, Kbin)
% compute annular intensity historgam
% input:
%   QueryImg: input image
%   HistBin: the number of bins for the histogram
%   Kbin: the number of annular histogram
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

TestHistMatrix = zeros(size(QueryImg, 3)*numBin, Kbin);
TestHistMatrixNorm = zeros(size(QueryImg, 3)*numBin, Kbin);
xA = [1 Width]; xB = xA;
yA = [1 Height]; yB =yA;
WidthBinX = floor((Width-1)/(2*Kbin));
WidthBinY = floor((Height-1)/(2*Kbin));
for iKbin = 1:Kbin
    rectangle('Position',[xA(1) yA(1) xA(2)-xA(1)+1 yA(2)-yA(1)+1],'LineWidth',2,'EdgeColor','g');    
    TestHist = zeros(numBin*size(QueryImg, 3), 1);
    %TestHistNorm = zeros(numBin*size(QueryImg, 3), 1);
    if (iKbin == Kbin)
        for iChn = 1:size(QueryImg, 3)
            ChnImg = QueryImg(:,:,iChn);
            vhistA = ChnImg(yA(1):yA(2), xA(1):xA(2));
            shistA = histc(vhistA(:), HistBin);
            shist = shistA; shist(end) = [];
            TestHist(numBin*(iChn-1)+1:numBin*iChn) = shist';
            %shist = shist/sum(shist); % may be normalize after concatenation
            %TestHistNorm(numBin*(iChn-1)+1:numBin*iChn) = shist';
        end
    else
        xB = [xA(1)+WidthBinX xA(2)-WidthBinX];
        yB = [yA(1)+WidthBinY yA(2)-WidthBinY];
        for iChn = 1:size(QueryImg, 3)
            ChnImg = QueryImg(:,:,iChn);
            vhistA = ChnImg(yA(1):yA(2), xA(1):xA(2));
            shistA = histc(vhistA(:), HistBin);
            vhistB = ChnImg(yB(1):yB(2), xB(1):xB(2));
            shistB = histc(vhistB(:), HistBin);
            shist = shistA - shistB; shist(end) = [];
            TestHist(numBin*(iChn-1)+1:numBin*iChn) = shist';
            %shist = shist/sum(shist); % may be normalize after concatenation
            %TestHistNorm(numBin*(iChn-1)+1:numBin*iChn) = shist';
        end
    end 
    TestHistNorm = TestHist/sum(TestHist(:));
    TestHistNorm(isnan(TestHistNorm)) = 0;
    TestHistMatrix(:,Kbin - iKbin + 1) = TestHist;
    TestHistMatrixNorm(:,Kbin - iKbin + 1) = TestHistNorm;
    xA = xB;
    yA = yB;
end

spaQueryHistMatrix = (TestHistMatrix(:));
spaQueryHistMatrixNorm = (TestHistMatrixNorm(:));
clear TestHistMatrixNorm TestHistMatrix TestHist
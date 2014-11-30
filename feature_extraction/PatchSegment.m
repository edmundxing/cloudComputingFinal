%% Switch function
function SegPatch = PatchSegment(SLChnTestImg, x, y, Ksegment, SegLabel)

% specificy which segment needs to be used for histogram
% input:
%   SLChnTestImg: image
%   x, y: the point positions
%   Ksegment: the number of segment
%   SegLabel: segment index

CentX = round((x(1)+x(2))/2);
CentY = round((y(1)+y(2))/2);

if (Ksegment == 8)
    switch SegLabel
        case 1
            c = [CentX x(2) x(2)];
            r = [CentY CentY y(1)];
            SegPatch = roipoly(SLChnTestImg(:,:,1), c, r);
        case 2
            c = [CentX x(2) CentX];
            r = [CentY y(1) y(1)];
            SegPatch = roipoly(SLChnTestImg(:,:,1), c, r);
        case 3
            c = [CentX CentX x(1)];
            r = [CentY y(1) y(1)];
            SegPatch = roipoly(SLChnTestImg(:,:,1), c, r);
        case 4
            c = [CentX x(1) x(1)];
            r = [CentY y(1) CentY];
            SegPatch = roipoly(SLChnTestImg(:,:,1), c, r);
        case 5
            c = [CentX x(1) x(1)];
            r = [CentY CentY y(2)];
            SegPatch = roipoly(SLChnTestImg(:,:,1), c, r);
        case 6
            c = [CentX x(1) CentX];
            r = [CentY y(2) y(2)];
            SegPatch = roipoly(SLChnTestImg(:,:,1), c, r);
        case 7
            c = [CentX CentX x(2)];
            r = [CentY y(2) y(2)];
            SegPatch = roipoly(SLChnTestImg(:,:,1), c, r);
        case 8
            c = [CentX x(2) x(2)];
            r = [CentY y(2) CentY];
            SegPatch = roipoly(SLChnTestImg(:,:,1), c, r);
        otherwise
            c = [x(1) x(1) x(2) x(2)];
            r = [y(1) y(2) y(2) y(1)];
            SegPatch = roipoly(SLChnTestImg(:,:,1), c, r);
    end
end
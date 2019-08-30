#include <iostream>
#include <fstream>
#include <iomanip>
#include <opencv2/core/core.hpp>
#include <opencv2/features2d/features2d.hpp>
#include <opencv2/xfeatures2d.hpp>
#include <opencv2/highgui/highgui.hpp>
#include <opencv2/calib3d/calib3d.hpp>
#include <Eigen/Geometry>


using namespace std;
using namespace cv;


bool LoadDescribeBinary(const string& filename, cv::Mat& output);

bool readDescribeBinary(ifstream& ifs, cv::Mat& in_mat);

bool readKeyPointBinary(ifstream& ifs, vector<KeyPoint>&  key_points);

bool LoadKeyPointBinary(const string& filename, vector<KeyPoint>&  output);

void extractDescriptor(const Mat &img, std::vector<KeyPoint>& keypoints, Mat& descriptor_1);

void picMatch(Mat &descriptor_1, Mat &descriptor_2,
              std::vector<DMatch> &knn_matches,
              std::vector<DMatch> &matches);

Point2f pixel2cam(const Point2d &p, const Mat &K);


bool poseEstimate(const std::vector<KeyPoint> &keypoints_1,
                  const std::vector<KeyPoint> &keypoints_2,
                  const std::vector<DMatch> &match,
                  Mat &R, Mat &T,Mat &mask);



int main(int arc, char **argv) {


    double x, y, z, threshold;

    string root_path = argv[1];

    string file_name = argv[2];

    
    //Mat img_1 = imread(argv[1],IMREAD_COLOR);

    Mat img_2 = imread(argv[3], IMREAD_COLOR);

    x = atof(argv[4]);

    y = atof(argv[5]);

    z = atof(argv[6]);

    threshold = atof(argv[7]);

    string feature_name = argv[8],temp_name;

    if(feature_name == "SIFT"){
         temp_name = "_sift";
    }else{
        temp_name = "_orb";
    }


    vector<KeyPoint> key_points_map;
    vector<KeyPoint>  key_points_cur;

    Mat descriptor_map;
    Mat descriptor_cur;

    string dec_save_path = root_path + "dec/"+file_name+temp_name+"_des.db";
    LoadDescribeBinary(dec_save_path, descriptor_map);

    string kp_save_path = root_path + "kp/"+file_name+temp_name+"_kp.db";
    LoadKeyPointBinary(kp_save_path, key_points_map);


    //extractDescriptor(img_1, key_points_map, descriptor_map);

    extractDescriptor(img_2, key_points_cur, descriptor_cur);

    Mat R, T,Mask;

    vector<DMatch> knn_matches,matches;

    picMatch(descriptor_map, descriptor_cur, knn_matches,matches);


    if (double(matches.size())/double(knn_matches.size())*100<threshold)
    {

       cout<<"False"<<endl;

       return 0;
    }


    bool pose=poseEstimate(key_points_map, key_points_cur, matches, R, T,Mask);
    if(pose)
    {


       Eigen::Matrix3d rotation;

       rotation << R.at<double>(0, 0), R.at<double>(0, 1), R.at<double>(0, 2),

                R.at<double>(1, 0), R.at<double>(1, 1), R.at<double>(1, 2),

                R.at<double>(2, 0), R.at<double>(2, 1), R.at<double>(2, 2);

       cout << rotation.eulerAngles(2, 1, 0) << endl;

       cv::Matx13f t;

       t<<T.at<double>(0,0)/10+x,T.at<double>(1,0)/10+y,T.at<double>(2,0)/10+z;

       cout << t<< endl;

    }
    else{


       cout<<"False"<<endl;
    }

}



void extractDescriptor(const Mat &img, std::vector<KeyPoint>& keypoints, Mat& descriptor_1) {



    bool SelectiveDescMethods =true;
    // 默认选择BRIEF描述符
    if (SelectiveDescMethods)

    {

        //Ptr<FeatureDetector> detector = ORB::create(1000, 1.2, 8, 31, 0, 2,ORB::HARRIS_SCORE, 31, 20);
        Ptr<FeatureDetector> detector = ORB::create();

        detector->detect(img, keypoints);

        Ptr<DescriptorExtractor> descriptor = ORB::create();

        descriptor->compute(img, keypoints, descriptor_1);
    }
    else
    {
         Ptr<Feature2D> sift = xfeatures2d::SIFT::create(0, 3, 0.04, 10);
         sift->detect(img, keypoints);
         sift->compute(img, keypoints, descriptor_1);


    }

}




void picMatch(Mat &query, Mat &train,
              std::vector<DMatch>& knn_matches,
              std::vector<DMatch>& matches)
{

    bool match_method=true;

    if(match_method)
    {
    Ptr<DescriptorMatcher> matcher = DescriptorMatcher::create("BruteForce-Hamming");

    matcher->match(query, train, knn_matches);

    int size = int(knn_matches.size());

    for (int i = 0; i <size; i++)
    {


        if (knn_matches[i].distance < 30) {


            matches.push_back(knn_matches[i]);

        }

    }
    }

    else
    {
    vector<vector<DMatch>> knn_matches;
    BFMatcher matcher(NORM_L2);
    matcher.knnMatch(query, train, knn_matches, 2);

    // 获取满足Ratio Test的最小匹配的距离
    float min_dist = FLT_MAX;
    for (int r = 0; r < knn_matches.size(); ++r)
    {
        // Rotio Test
        if (knn_matches[r][0].distance > 0.75 * knn_matches[r][1].distance)
        {
            continue;
        }

        float dist = knn_matches[r][0].distance;
        if (dist < min_dist)
        {
            min_dist = dist;
        }
    }

    matches.clear();
    for (size_t r = 0; r < knn_matches.size(); ++r)
    {

        // 排除不满足Ratio Test的点和匹配距离过大的点
        if (
                knn_matches[r][0].distance > 0.75 * knn_matches[r][1].distance ||
                knn_matches[r][0].distance > 5* max(min_dist, 10.0f)
                )
        {
            continue;
        }

        //保存匹配点
        matches.push_back(knn_matches[r][0]);
    }
    }



}

Point2f pixel2cam(const Point2d &p, const Mat &K) {
    return Point2f
            (
                    (p.x - K.at<double>(0, 2)) / K.at<double>(0, 0),
                    (p.y - K.at<double>(1, 2)) / K.at<double>(1, 1)
            );
}


bool poseEstimate(const std::vector<KeyPoint> &keypoints_1,
                  const std::vector<KeyPoint> &keypoints_2,
                  const std::vector<DMatch> &match,
                  Mat &R,
                  Mat &T,
                  Mat &mask)
{


    vector<Point2f> point_1, point_2;

    for (int i = 0; i < int(match.size()); i++) {

        point_1.push_back(keypoints_1[match[i].queryIdx].pt);

        point_2.push_back(keypoints_2[match[i].trainIdx].pt);
    }


    Point2d principal_point(319.3, 234.4);
    int focal_length = 546.3;
    Mat essential_matrix;
    essential_matrix = findEssentialMat(point_1, point_2, focal_length, principal_point, RANSAC, 0.999, 1.0, mask);
    //-- 从本质矩阵中恢复旋转和平移信息.
    recoverPose(essential_matrix, point_1, point_2, R, T, focal_length, principal_point,mask);
    
    double feasible_count = countNonZero(mask);

    if (feasible_count <= 15 || (feasible_count /point_1.size()) < 0.5)
    {
       return false;
    }
    
    return true;


}





//! Read cv::Mat from binary

//\param[in] ifs input file stream
//\param[out] in_mat mat to load

bool readDescribeBinary(ifstream& ifs, cv::Mat& in_mat)
{
    if(!ifs.is_open()){
        return false;
    }

    int rows, cols, type;
    ifs.read((char*)(&rows), sizeof(int));
    if(rows==0){
        return true;
    }
    ifs.read((char*)(&cols), sizeof(int));
    ifs.read((char*)(&type), sizeof(int));

    in_mat.release();

    in_mat.create(rows, cols, type);

    ifs.read((char*)(in_mat.data), in_mat.elemSize() * in_mat.total());

    return true;
}


//! Load cv::Mat as binary

//\param[in] filename filaname to load
//\param[out] output loaded cv::Mat

bool LoadDescribeBinary(const string& filename, cv::Mat& output){

    ifstream ifs(filename, ios::binary);
    return readDescribeBinary(ifs, output);
}




bool readKeyPointBinary(ifstream& ifs, vector<KeyPoint>&  key_points)
{
    if(!ifs.is_open()){
        return false;
    }

    int size1;
    ifs.read((char*)&size1, 4);
    key_points.resize(size1);
    ifs.read((char*)&key_points[0], size1 * sizeof(key_points));


    return true;
}


//! Load cv::Mat as binary

//\param[in] filename filaname to load
//\param[out] output loaded cv::Mat

bool LoadKeyPointBinary(const string& filename, vector<KeyPoint>&  output){

    ifstream ifs(filename, ios::binary);

    return readKeyPointBinary(ifs, output);
}

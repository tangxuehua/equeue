function PostController($scope, $http) {
    $scope.sections = [];
    $scope.errorMsg = '';

    $scope.showNewPostDialog = function () {
        $scope.errorMsg = '';
        $scope.newPost = {
            subject: '',
            body: '',
            sectionId: sectionId
        };
        if ($scope.sections.length > 0) {
            $("#float-box-newPost").modal("show");
            return;
        }

        $http({
            method: 'GET',
            url: '/section/getall'
        })
        .success(function (result, status, headers, config) {
            if (result.success) {
                $scope.sections = result.data;
                $("#float-box-newPost").modal("show");
            } else {
                msg(result.errorMsg);
            }
        })
        .error(function (result, status, headers, config) {
            msg(result.errorMsg);
        });
    };

    $scope.submitPost = function () {
        if (isStringEmpty($scope.newPost.sectionId)) {
            $scope.errorMsg = '请选择帖子所属版块。';
            return false;
        }
        if (isStringEmpty($scope.newPost.subject)) {
            $scope.errorMsg = '请输入帖子标题。';
            return false;
        }
        if (isStringEmpty($scope.newPost.body)) {
            $scope.errorMsg = '请输入帖子内容。';
            return false;
        }

        $http({
            method: 'POST',
            url: '/post/create',
            data: $scope.newPost
        })
        .success(function (result, status, headers, config) {
            if (result.success) {
                //showAutoCloseAlert();
                //$("#float-box-newPost").modal("hide");
                window.location.reload();
            } else {
                $scope.errorMsg = result.errorMsg;
            }
        })
        .error(function (result, status, headers, config) {
            $scope.errorMsg = result.errorMsg;
        });
    };
}
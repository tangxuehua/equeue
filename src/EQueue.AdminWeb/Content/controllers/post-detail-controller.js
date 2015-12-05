function PostDetailController($scope, $http) {
    $scope.errorMsg = '';

    $scope.showNewReplyDialog = function () {
        $scope.errorMsg = '';
        $scope.newReply = {
            body: '',
            postId: postId,
            parentId: ''
        };
        $("#float-box-newReply").modal("show");
    };
    $scope.showNewSubReplyDialog = function (parentId) {
        $scope.errorMsg = '';
        $scope.newReply = {
            body: '',
            postId: postId,
            parentId: parentId
        };
        $("#float-box-newReply").modal("show");
    };
    $scope.showEditPostDialog = function () {
        $scope.errorMsg = '';

        $http({
            method: 'GET',
            url: '/post/find',
            cache: false,
            params: { id: postId, option: 'simple', random: Math.random() }
        })
        .success(function (result, status, headers, config) {
            if (result.success) {
                $scope.editingPost = result.data;
                $("#float-box-editingPost").modal("show");
            } else {
                msg(result.errorMsg);
            }
        })
        .error(function (result, status, headers, config) {
            msg(result.errorMsg);
        });
    };
    $scope.showEditReplyDialog = function (replyId) {
        $scope.errorMsg = '';

        $http({
            method: 'GET',
            url: '/reply/find',
            cache: false,
            params: { id: replyId, option: 'simple', random: Math.random() }
        })
        .success(function (result, status, headers, config) {
            if (result.success) {
                $scope.editingReply = result.data;
                $("#float-box-editingReply").modal("show");
            } else {
                msg(result.errorMsg);
            }
        })
        .error(function (result, status, headers, config) {
            msg(result.errorMsg);
        });
    };

    $scope.submitReply = function () {
        if (isStringEmpty($scope.newReply.body)) {
            $scope.errorMsg = '请输入回复内容。';
            return false;
        }
        if (isStringEmpty($scope.newReply.postId)) {
            $scope.errorMsg = '回复对应的帖子ID不能为空。';
            return false;
        }

        $http({
            method: 'POST',
            url: '/reply/create',
            data: $scope.newReply
        })
        .success(function (result, status, headers, config) {
            if (result.success) {
                //showAutoCloseAlert();
                //$("#float-box-newReply").modal("hide");
                window.location.reload();
            } else {
                $scope.errorMsg = result.errorMsg;
            }
        })
        .error(function (result, status, headers, config) {
            $scope.errorMsg = result.errorMsg;
        });
    };

    $scope.updatePost = function () {
        if (isStringEmpty($scope.editingPost.subject)) {
            $scope.errorMsg = '请输入帖子标题。';
            return false;
        }
        if (isStringEmpty($scope.editingPost.body)) {
            $scope.errorMsg = '请输入帖子内容。';
            return false;
        }

        $http({
            method: 'POST',
            url: '/post/update',
            data: $scope.editingPost
        })
        .success(function (result, status, headers, config) {
            if (result.success) {
                showAutoCloseAlert();
                $("#float-box-editingPost").modal("hide");
            } else {
                $scope.errorMsg = result.errorMsg;
            }
        })
        .error(function (result, status, headers, config) {
            $scope.errorMsg = result.errorMsg;
        });
    };
    $scope.updateReply = function () {
        if (isStringEmpty($scope.editingReply.body)) {
            $scope.errorMsg = '请输入回复内容。';
            return false;
        }

        $http({
            method: 'POST',
            url: '/reply/update',
            data: $scope.editingReply
        })
        .success(function (result, status, headers, config) {
            if (result.success) {
                showAutoCloseAlert();
                $("#float-box-editingReply").modal("hide");
            } else {
                $scope.errorMsg = result.errorMsg;
            }
        })
        .error(function (result, status, headers, config) {
            $scope.errorMsg = result.errorMsg;
        });
    };
}
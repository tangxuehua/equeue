function SectionController($scope, $http) {
    $scope.errorMsg = '';

    $scope.showNewSectionDialog = function () {
        $scope.errorMsg = '';
        $scope.newSection = {
            name: ''
        };
        $("#float-box-newSection").modal("show");
    };
    $scope.showEditSectionDialog = function (sectionId) {
        $scope.errorMsg = '';

        $http({
            method: 'GET',
            url: '/sectionadmin/find',
            cache: false,
            params: { id: sectionId, option: 'simple', random: Math.random() }
        })
        .success(function (result, status, headers, config) {
            if (result.success) {
                $scope.editingSection = result.data;
                $("#float-box-editSection").modal("show");
            } else {
                msg(result.errorMsg);
            }
        })
        .error(function (result, status, headers, config) {
            msg(result.errorMsg);
        });
    };

    $scope.createSection = function () {
        if (isStringEmpty($scope.newSection.name)) {
            $scope.errorMsg = '请输入版块名称。';
            return false;
        }

        $http({
            method: 'POST',
            url: '/sectionadmin/create',
            data: $scope.newSection
        })
        .success(function (result, status, headers, config) {
            if (result.success) {
                window.location.reload();
            } else {
                $scope.errorMsg = result.errorMsg;
            }
        })
        .error(function (result, status, headers, config) {
            $scope.errorMsg = result.errorMsg;
        });
    };
    $scope.updateSection = function () {
        if (isStringEmpty($scope.editingSection.name)) {
            $scope.errorMsg = '请输入版块名称。';
            return false;
        }

        $http({
            method: 'POST',
            url: '/sectionadmin/update',
            data: $scope.editingSection
        })
        .success(function (result, status, headers, config) {
            if (result.success) {
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
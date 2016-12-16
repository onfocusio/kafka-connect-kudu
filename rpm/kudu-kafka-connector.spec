%define install_path /usr/share/java/kafka-serde-tools

# RVM can not be sourced with default /bin/sh
%define _buildshell /bin/bash

Name:           kudu-kafka-connector
Version:        0.0.0
Release:        SNAPSHOT.el7_AF
Summary:        Kudu Kafka connector
License:        Kudu
URL:            https://github.com/onfocusio/kafka-connect-kudu
Source0:        %{name}-%{version}.tar

# http://wiki.mandriva.com/en/Development/Packaging/Groups
#Group:         OnFocus
Vendor:         Onfocus.io

# BuildRoot is ignored in spec files from rpm>=4.6
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-%(%{__id_u} -n)

#BuildArch:      noarch

#BuildRequires:  some_rpm

#Requires:       some_rpm

# http://www.rpm.org/max-rpm-snapshot/s1-rpm-inside-tags.html
#AutoReq: no
#AutoProv: no


# http://www.rpm.org/max-rpm-snapshot/s1-rpm-specref-macros.html

%description
Kudu Kafka connector


%prep

# test -x /bin/mvn || ( echo Missing /bin/mvn ; exit -1 )


%setup -q


%build


./gradlew clean assemble

#git log >CHANGELOG


%install
# %{buildroot} and $RPM_BUILD_ROOT are same
test -n "$RPM_BUILD_ROOT" || exit -1

rm -rf %{buildroot}

INSTALL_PATH=%{install_path}

# Install project files
mkdir -p ${RPM_BUILD_ROOT}${INSTALL_PATH}

pushd build/libs
tar --exclude-vcs --exclude ./rpmbuild --exclude ./rpm --exclude ./rpmtmp -cp kafka-connect-kudu-*.jar | (cd ${RPM_BUILD_ROOT}${INSTALL_PATH}/; tar xp)
popd

pushd ${RPM_BUILD_ROOT}${INSTALL_PATH}/
mv `ls -1 kafka-connect-kudu-*.jar |sort -nr |head -1` `ls -1 kafka-connect-kudu-*.jar |sort -nr |head -1 |sed 's/^kafka-connect-kudu-/kudu-kafka-connector-/'`
# ln -s `ls -1 kudu-kafka-connector-*.jar |sort -nr |head -1` kudu-kafka-connector.jar
popd

# Strip .git dirs
#find ${RPM_BUILD_ROOT}${INSTALL_PATH} -name .git -type d -prune -exec rm -rf {} \;

# Strip binaries
#find ${RPM_BUILD_ROOT}${INSTALL_PATH}/ -type f -print0 |xargs -0 -r file --no-dereference --no-pad |grep 'not stripped' |cut -f1 -d: |xargs -r -n1 strip -g

# Fix bad paths in generated files
#find ${RPM_BUILD_ROOT}${INSTALL_PATH}/ -type f -name Makefile -print0 |xargs -0 -r -n1 sed -i "s#${RPM_BUILD_ROOT}##g"
#find ${RPM_BUILD_ROOT}${INSTALL_PATH}/ -type f -print0 |xargs -0 -r -n1 sed -i "s#${RPM_BUILD_ROOT}##g"


%clean

rm -rf %{buildroot}


# Pre-install script
#%pre


# Post-install script
#%post


# Pre-uninstall script
#%preun


# Post-uninstall script
#%postun


%files
# http://www.rpm.org/max-rpm-snapshot/s1-rpm-specref-files-list.html
# http://www.rpm.org/max-rpm-snapshot/s1-rpm-specref-files-list-directives.html
# http://www.rpm.org/max-rpm-snapshot/s1-rpm-anywhere-specifying-file-attributes.html

%defattr(0644, root, root, 0755)
%{install_path}


%changelog
* Fri Dec 16 2016 Alexandre Fouche <alexandre.fouche@gmail.com>
- Initial spec file

%define         debug_package %{nil}

%define         minio_tag RELEASE.2017-09-29T19-16-56Z
%define         minio_subver %(echo %{tag} | sed -e 's/[^0-9]//g')
# define	  minio_commitid DEFINEME
%define		minio_import_path github.com/minio/minio

%define		charon_version 1.1
# define	  charon_commitid DEFINEME
%define		charon_import_path github.com/cvmfs/docker-graphdriver/publisher

Summary:        CernVM-FS Server Portals Add-Ons
Name:           cvmfs-server-portals
Version:        0.9
Release:        1%{?dist}
Source0:        https://ecsft.cern.ch/cvmfs/docker-graphdriver-%{charon_version}.tar.gz
Source1:        https://github.com/cvmfs/minio/archive/%{minio_tag}.tar.gz
Group:          Applications/System
License:        BSD
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

BuildRequires: golang >= 1.8
Requires: cvmfs-server

%description
The portals extension for the CernVM-FS release manager machine allows for
creating named S3 enpoints into a repository.  It uses the Minio server
for S3 traffic.

%prep
%setup -qc
mkdir -p src/$(dirname %{minio_import_path})
ln -s ../../../cvmfs-minio-%{minio_tag} src/%{minio_import_path}

%setup -TDqa 1
mkdir -p src/$(dirname $(dirname %{charon_import_path}))
ln -s ../../../docker-graphdriver-%{charon_version} src/$(dirname %{charon_import_path})

%build
export GOPATH=$(pwd)

go build -o cvmfs_charon %{charon_import_path}

minio_tag=%{minio_tag}
minio_version=${minio_tag#RELEASE.}
minio_commitid=%{minio_commitid}
minio_scommitid=$(echo $commitid | head -c12)
minio_prefix=%{minio_import_path}/cmd

MINIO_LDFLAGS="
-X $minio_prefix.Version=$minio_version
-X $minio_prefix.ReleaseTag=$minio_tag
-X $minio_prefix.CommitID=$minio_commitid
-X $minio_prefix.ShortCommitID=$minio_scommitid
"

go build -ldflags "${MINIO_LDFLAGS}" -o cvmfs_minio %{minio_import_path}

# check that version set properly
./cvmfs_minio version | tee v
v=$(awk '/Version:/{print $2}' v)
test "$v" = $minio_version
v=$(awk '/Release-Tag:/{print $2}' v)
test "$v" = $minio_tag
v=$(awk '/Commit-ID:/{print $2}' v)
test "$v" = $minio_commitid

%install
rm -rf $RPM_BUILD_ROOT
install -d $RPM_BUILD_ROOT%{_bindir}
install -d ${RPM_BUILD_ROOT}/usr/lib/systemd/system
install -p cvmfs_minio $RPM_BUILD_ROOT%{_bindir}
install -p cvmfs_charon $RPM_BUILD_ROOT%{_bindir}
install -p "%{charon_import_path}/cvmfs-portal@.service" ${RPM_BUILD_ROOT}/usr/lib/systemd/system

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root)
%{_bindir}/cvmfs_minio
%{_bindir}/cvmfs_charon
/usr/lib/systemd/system/cvmfs-portal@.service

%changelog
* Tue Oct 10 2017 Jakob Blomer <jblomer@cern.ch> - 0.9
- Initial packaging

Summary: t-storage-overlaybd-tcmu
Name: t-storage-overlaybd-tcmu
Version:1.0.0
Release: %{_ver}%{dist}
Group: Development/Libraries
License: GPL
URL: http://www.aliyun.com

BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root
Autoreq: 0
%description
This is the target_core_user kernel module.

%prep
rm -rf %{buildroot}

%install
kernel_unames=%{_kernel_unames}
for k in ${kernel_unames//,/ }; do
    mkdir -p $RPM_BUILD_ROOT/lib/modules/$k/updates
    install -m0644 %{_rpmdir}/lib/$k/target_core_user.ko $RPM_BUILD_ROOT/lib/modules/$k/updates/
done

%post

kernel_unames=%{_kernel_unames}
for k in ${kernel_unames//,/ }; do
    /sbin/depmod -a ${k} >/dev/null 2>&1
done

echo ">>>>>> Modprobe target_core_user module for `uname -r`"
/sbin/rmmod target_core_user &>/dev/null
modprobe target_core_user
echo ">>>>>> Install target_core_user module done"

%preun

%postun

kernel_unames=%{_kernel_unames}
for k in ${kernel_unames//,/ }; do
    /sbin/depmod -a ${k} >/dev/null 2>&1
done

# Do nothing when it's an update operation($1 is 1)
if [ "$1" != "0" ]; then
    exit
fi

/sbin/rmmod target_core_user &>/dev/null


%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root)
/lib/modules

%changelog